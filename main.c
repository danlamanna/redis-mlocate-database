#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <ftw.h>
#include <errno.h>

#include <hiredis/hiredis.h>
#include <openssl/md5.h>

#define REDIS_SIZE_STR "size"
#define REDIS_MTIME_STR "mtime"
#define REDIS_MD5SUM_STR "md5sum"

#define REDIS_COUNT_UPDATED 0
#define REDIS_COUNT_ADDED 1
#define REDIS_COUNT_DELETED 2

redisContext *redis;
int dry_run = 0;
int counts[3] = {0};

// Based off: http://stackoverflow.com/a/10324904/520929
char *md5sum(const char *fpath) {
  unsigned char *c = malloc(sizeof(unsigned char) * MD5_DIGEST_LENGTH);
  int i;
  FILE *inFile = fopen (fpath, "rb");
  MD5_CTX mdContext;
  int bytes;
  unsigned char data[1024];

  if (inFile == NULL) {
    printf ("%s can't be opened.\n", fpath);
    return 0;
  }

  MD5_Init (&mdContext);
  while ((bytes = fread (data, 1, 1024, inFile)) != 0)
    MD5_Update (&mdContext, data, bytes);
  MD5_Final (c,&mdContext);

  fclose (inFile);

  char *md5string = malloc(sizeof(char) * (32 + 1));
  for(i = 0; i < 16; ++i)
    sprintf(&md5string[i*2], "%02x", (unsigned int)c[i]);

  return md5string;
}

int _redis_add_file(const char *fpath, const struct stat *sb) {
  redisReply *reply;

  if (!dry_run)
    reply = redisCommand(redis, "HMSET %s "
                                "mtime %d "
                                "size %d "
                                "md5sum %s",
      fpath, sb->st_mtim.tv_sec, sb->st_size, md5sum(fpath));

  return 1;
}

int redis_upsert_file(const char *fpath, const struct stat *sb, int typeflag) {
  int n;
  unsigned char *out;
  redisReply *reply;
  struct tm *mytm;

  // We only consider files, and files > size 0
  if (typeflag != FTW_F || sb->st_size == 0)
    return 0;

  reply = redisCommand(redis, "HGET %s mtime", fpath);

  if (reply->type == REDIS_REPLY_NIL) {
    printf("Adding %s to redis.\n", fpath);
    _redis_add_file(fpath, sb);

    counts[REDIS_COUNT_ADDED]++;
  } else {
    // Already exists, lets check mtime
    reply = redisCommand(redis, "HGET %s mtime", fpath);

    if (atoi(reply->str) != sb->st_mtim.tv_sec) {
      printf("Updating %s.\n", fpath);
      _redis_add_file(fpath, sb);

      counts[REDIS_COUNT_UPDATED]++;
    }
  }

  return 0;
}

void remove_stale_redis_keys(redisContext *redis) {
  redisReply *reply;
  int i;

  reply = redisCommand(redis, "keys %s", "*");

  for (i=0; i < reply->elements; i++) {
    if (access(reply->element[i]->str, F_OK) == -1) {
      if (!dry_run)
        redisCommand(redis, "del %s", reply->element[i]->str, F_OK);

      counts[REDIS_COUNT_DELETED]++;
    }
  }
}

int main(int argc, char *argv[]) {
  redisReply *reply;
  int deleted_keys_count = 0;

  if (argc < 2) {
    printf("Usage: redis-checksum directory [--dry-run]\n");
    return 1;
  }

  if (argc > 2 && (strcmp(argv[2], "--dry-run") == 0)) {
    dry_run = 1;
  }


  redis = redisConnect("127.0.0.1", 6379);

  if(redis->err) {
    fprintf(stderr, "Connection error: %s\n", redis->errstr);
    exit(EXIT_FAILURE);
  }

  // Get rid of non-existent files from redis
  remove_stale_redis_keys(redis);

  ftw(argv[1], &redis_upsert_file, 10);

  printf("Summary%s:\n", (dry_run) ? " (DRY RUN)" : "");
  printf("Keys Added:   %d\n", counts[REDIS_COUNT_ADDED]);
  printf("Keys Updated: %d\n", counts[REDIS_COUNT_UPDATED]);
  printf("Keys Deleted: %d\n", counts[REDIS_COUNT_DELETED]);

  return 0;
}
