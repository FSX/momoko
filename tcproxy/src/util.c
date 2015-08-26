#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <string.h>

#include "util.h"

static LogLevel log_level = kDebug;
static FILE *log_file = NULL;
static char now_str[sizeof("2011/11/11 11:11:11")];
static const char *LevelName[] = {
  "NONE",
  "FATAL",
  "CRITICAL",
  "ERROR",
  "WARNING",
  "INFO",
  "DEBUG",
};

static void UpdateTime() {
  static time_t now = 0;
  time_t t = time(NULL);

  //update time every second
  if (t - now == 0) return;
  now = t;

  struct tm tm;
  localtime_r(&now, &tm);
  sprintf(now_str, "%04d/%02d/%02d %02d:%02d:%02d",
      1900 + tm.tm_year, tm.tm_mon + 1, tm.tm_mday, 
      tm.tm_hour, tm.tm_min, tm.tm_sec);
}

void LogPrint(LogLevel level, const char *fmt, ...) {
  va_list  args;
  if (level > log_level) return;
  va_start(args, fmt);
  if (log_file) vfprintf(log_file, fmt, args);
  va_end(args);
  fflush(log_file);
}

void LogInternal(LogLevel level, const char *fmt, ...) {
  va_list  args;
  if (level > log_level) return;
  UpdateTime();
  if (log_file) fprintf(log_file, "%s [%s] ", now_str, LevelName[level]);
  va_start(args, fmt);
  if (log_file) vfprintf(log_file, fmt, args);
  va_end(args);
  fflush(log_file);
}

void InitLogger(LogLevel level, const char *filename) {
  log_level = level;

  if (filename == NULL || strcmp(filename, "stderr") == 0 || strcmp(filename, "") == 0) {
    log_file = stderr;
  } else if (strcmp(filename, "stdout") == 0) {
    log_file = stdout;
  } else {
    log_file = fopen(filename, "a+");
  }
}


void Daemonize() {
  int fd;

  if (fork() != 0) exit(0); /* parent exits */

  setsid(); /* create a new session */

  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    if (fd > STDERR_FILENO) close(fd);
  }
}

BufferList *AllocBufferList(int n) {
  BufferList *blist = malloc(sizeof(BufferList));
  BufferListNode *buf = malloc(sizeof(BufferListNode));
  BufferListNode *pre;
  int i;

  buf->size = 0;
  buf->next = NULL;

  blist->head = buf;
  pre = blist->head = blist->write_node = buf;

  for (i = 1; i < n; i++) {
    buf = malloc(sizeof(BufferListNode));
    buf->size = 0;
    buf->next = NULL;
    pre->next = buf;
    pre = buf;
  }

  blist->tail = buf;

  blist->read_pos = 0;

  return blist;
}

void FreeBufferList(BufferList *blist) {
  BufferListNode *cur = blist->head;
  while (cur != NULL) {
    blist->head = cur->next;
    free(cur);
    cur = blist->head;
  }
  free(blist);
}

// get free space from current write node
char *BufferListGetSpace(BufferList *blist, int *len) {
  if (blist->write_node == blist->tail && blist->write_node->size == BUFFER_CHUNK_SIZE) {
    *len = 0;
    LogDebug("tail full");
    return NULL;
  }
  *len = BUFFER_CHUNK_SIZE - blist->write_node->size;
  return blist->write_node->data + blist->write_node->size;
}

// push data into buffer
void BufferListPush(BufferList *blist, int len) {
  blist->write_node->size += len;
  LogDebug("head %p tail %p cur %p data %d", blist->head, blist->tail, blist->write_node, blist->head->size - blist->read_pos);
  if (blist->write_node->size == BUFFER_CHUNK_SIZE && blist->write_node != blist->tail) {
    // move to next chunk
    blist->write_node = blist->write_node->next;
  }
}

// always get data from head
char *BufferListGetData(BufferList *blist, int *len) {
  if (blist->head == blist->write_node && blist->read_pos == blist->head->size) {
    *len = 0;
    LogDebug("head empty");
    return NULL;
  }
  *len = blist->head->size - blist->read_pos;
  return blist->head->data + blist->read_pos;
}

// pop data out from buffer
void BufferListPop(BufferList *blist, int len) {
  blist->read_pos += len;
  LogDebug("head %p tail %p cur %p data %d", blist->head, blist->tail, blist->write_node, blist->head->size - blist->read_pos);
  if (blist->read_pos == blist->head->size && blist->head != blist->write_node) {
    // head empty, and head is not the node we are writing into, move to tail
    BufferListNode *cur = blist->head;
    blist->head = blist->head->next;
    blist->tail->next = cur;
    blist->tail = cur;
    cur->size = 0;
    cur->next = NULL;
    blist->read_pos = 0;
    if (blist->head == NULL) {
      // there is only one chunk in buffer list
      LogDebug("head null");
      exit(0);
      blist->head = blist->tail;
    }
  }
  // else leave it there, further get data will return NULL
}
