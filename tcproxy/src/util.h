#ifndef _UTIL_H_
#define _UTIL_H_

#define BUFFER_CHUNK_SIZE 1024*1024*2

typedef enum LogLevel {
  kNone = 0,
  kFatal,
  kCritical,
  kError,
  kWarning,
  kInfo,
  kDebug,
} LogLevel;

#define LogInfo(s...) do {\
  LogInternal(kInfo, s);\
  LogPrint(kInfo, "\n"); \
}while(0)

#define LogWarning(s...) do {\
  LogInternal(kWarning, s);\
  LogPrint(kWarning, "\n"); \
}while(0)

#define LogError(s...) do {\
  LogInternal(kError, s);\
  LogPrint(kError, "\n"); \
}while(0)

#define LogCritical(s...) do {\
  LogInternal(kCritical, s);\
  LogPrint(kCritical, "\n"); \
}while(0)

#define LogFatal(s...) do {\
  LogInternal(kFatal, s);\
  LogPrint(kFatal, "\n"); \
  exit(EXIT_FAILURE);\
}while(0)

#ifdef DEBUG
#define LogDebug(s...) do {\
  LogInternal(kDebug, s);\
  LogPrint(kDebug, " [%s]", __PRETTY_FUNCTION__);\
  LogPrint(kDebug, "\n"); \
}while(0)
#else
#define LogDebug(s...)
#endif

void InitLogger(LogLevel level, const char *filename);
void LogInternal(LogLevel level, const char *fmt, ...);
void LogPrint(LogLevel level, const char *fmt, ...);

typedef struct BufferListNode {
  char data[BUFFER_CHUNK_SIZE];
  int size;
  struct BufferListNode *next;
} BufferListNode;

typedef struct BufferList {
  BufferListNode *head;
  BufferListNode *tail;
  int read_pos;
  BufferListNode *write_node;
} BufferList;

BufferList *AllocBufferList(int n);

void FreeBufferList(BufferList *blist);
char *BufferListGetData(BufferList *blist, int *len);
char *BufferListGetSpace(BufferList *blist, int *len);
void BufferListPop(BufferList *blist, int len);
void BufferListPush(BufferList *blist, int len);

void Daemonize();

#endif /* _UTIL_H_ */
