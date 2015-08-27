#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/socket.h>

#include "policy.h"
#include "util.h"
#include "ae.h"
#include "anet.h"

#define MAX_WRITE_PER_EVENT 1024*1024*1024
#define CLIENT_CLOSE_AFTER_SENT 0x01
#define VERSION "0.9.2"

Policy *policy;
static int run_daemonize = 0;
static char error_[1024];
aeEventLoop *el;

typedef struct Client {
  int fd;
  int flags;

  struct Client *remote;
  BufferList *blist;

  void (*OnError)(struct Client *c);
  void (*OnRemoteDown)(struct Client *c);
} Client;

void FreeRemote(Client *c);
void ReadIncome(aeEventLoop *el, int fd, void *privdata, int mask);

void Usage() {
  printf("usage:\n"
      "  tcproxy [options] \"proxy policy\"\n"
      "options:\n"
      "  -l file    specify log file\n"
      "  -d         run in background\n"
      "  -v         show detailed log\n"
      "  --version  show version and exit\n"
      "  -h         show help and exit\n\n"
      "examples:\n"
      "  tcproxy \"11212 -> 11211\"\n"
      "  tcproxy \"127.0.0.1:6379 -> rr{192.168.0.100:6379 192.168.0.101:6379}\"\n\n"
      );
  exit(EXIT_SUCCESS);
}

void ParseArgs(int argc, char **argv) {
  int i, j;
  const char *logfile = "stderr";
  int loglevel = kError;

  InitLogger(loglevel, NULL);

  for (i = 1; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
        Usage();
      } else if (!strcmp(argv[i], "--version")) {
        printf("tcproxy "VERSION"\n\n");
        exit(EXIT_SUCCESS);
      } else if (!strcmp(argv[i], "-d")) {
        run_daemonize = 1;
      } else if (!strcmp(argv[i], "-l")) {
        if (++i >= argc) LogFatal("file name must be specified");
        logfile = argv[i];
      } else if (!strncmp(argv[i], "-v", 2)) {
        for (j = 1; argv[i][j] != '\0'; j++) {
          if (argv[i][j] == 'v') loglevel++;
          else LogFatal("invalid argument %s", argv[i]);;
        }
      } else {
        LogFatal("unknow option %s\n", argv[i]);
      }
    } else {
      policy = ParsePolicy(argv[i]);
    }
  }

  InitLogger(loglevel, logfile);

  if (policy == NULL) {
    LogFatal("policy not valid");
  }
}

void SignalHandler(int signo) {
  if (signo == SIGINT || signo == SIGTERM) {
    el->stop = 1;
  }
}

void RemoteDown(Client *r) {
  r->remote->OnRemoteDown(r->remote);
}

Client *AllocRemote(Client *c) {
  Client *r = malloc(sizeof(Client));
  r->flags = 0;
  int fd = anetTcpNonBlockConnect(error_, policy->hosts[0].addr, policy->hosts[0].port);

  if (r == NULL || fd == -1) return NULL;
  LogDebug("connect remote fd %d", fd);
  anetNonBlock(NULL, fd);
  anetTcpNoDelay(NULL, fd);
  r->fd = fd;
  r->remote = c;
  r->OnError = RemoteDown;
  r->blist = AllocBufferList(3);
  if (aeCreateFileEvent(el, r->fd, AE_READABLE, ReadIncome, r) == AE_ERR) {
    close(fd);
    return NULL;
  }

  LogDebug("new remote %d %d", r->fd, c->fd);

  return r;
}

void FreeClient(Client *c) {
  if (c == NULL) return;
  LogDebug("free client %d", c->fd);
  aeDeleteFileEvent(el, c->fd, AE_READABLE);
  aeDeleteFileEvent(el, c->fd, AE_WRITABLE);
  close(c->fd);
  FreeRemote(c->remote);
  FreeBufferList(c->blist);
  free(c);
}

void CloseAfterSent(Client *c) {
  int len;
  if (BufferListGetData(c->blist, &len) == NULL) {
    // no data remains to be sent, close this client
    FreeClient(c);
  } else {
    c->flags |= CLIENT_CLOSE_AFTER_SENT;
  }
}

void ReAllocRemote(Client *c) {
  // TODO
}

Client *AllocClient(int fd) {
  Client *c = malloc(sizeof(Client));
  c->flags = 0;
  if (c == NULL) return NULL;

  anetNonBlock(NULL, fd);
  anetTcpNoDelay(NULL, fd);

  c->fd = fd;
  c->blist = AllocBufferList(3);
  c->remote = AllocRemote(c);
  c->OnError = FreeClient;
  // c->OnRemoteDown = ReAllocRemote;
  c->OnRemoteDown = CloseAfterSent;  // freeclient temprarily before hot switch done
  if (c->remote == NULL) {
    close(fd);
    free(c);
    return NULL;
  }

  LogDebug("New client fd:%d remotefd:%d", c->fd, c->remote->fd);

  return c;
}

void FreeRemote(Client *r) {
  LogDebug("free remote");
  aeDeleteFileEvent(el, r->fd, AE_READABLE);
  aeDeleteFileEvent(el, r->fd, AE_WRITABLE);
  close(r->fd);
  FreeBufferList(r->blist);
  free(r);
}

void SendOutcome(aeEventLoop *el, int fd, void *privdata, int mask) {
  LogDebug("SendOutcome");
  Client *c = (Client*)privdata;
  int len, nwritten = 0, totwritten = 0;
  char *buf;

  buf = BufferListGetData(c->blist, &len);
  if (buf == NULL) {
    LogDebug("delete write event");
    aeDeleteFileEvent(el, fd, AE_WRITABLE);
  }
  
  while (1) {
    buf = BufferListGetData(c->blist, &len);
    if (buf == NULL) {
      // no data to send
      if (c->flags & CLIENT_CLOSE_AFTER_SENT) {
        FreeClient(c);
        return;
      }
      break;
    }
    nwritten = send(fd, buf, len, MSG_DONTWAIT);
    if (nwritten <= 0) break;

    totwritten += nwritten;
    LogDebug("write and pop data %p %d", c->blist, nwritten);
    BufferListPop(c->blist, nwritten);
    /* Note that we avoid to send more than MAX_WRITE_PER_EVENT
     * bytes, in a single threaded server it's a good idea to serve
     * other clients as well, even if a very large request comes from
     * super fast link that is always able to accept data*/
    if (totwritten > MAX_WRITE_PER_EVENT) break;
  }

  LogDebug("totwritten %d", totwritten);

  if (nwritten == -1) {
    if (errno == EAGAIN) {
      nwritten = 0;
    } else {
      LogDebug("write error %s", strerror(errno));
      c->OnError(c);
      return;
    }
  }
}

int SetWriteEvent(Client *c) {
  if (aeCreateFileEvent(el, c->fd, AE_WRITABLE, SendOutcome, c) == AE_ERR) {
    LogError("Set write event failed");
    return -1;
  }
  return 0;
}

void ReadIncome(aeEventLoop *el, int fd, void *privdata, int mask) {
  LogDebug("read in come");
  Client *c = (Client*)privdata;
  Client *r = c->remote;
  char *buf;
  int len, nread = 0;

  while (1) {
    buf = BufferListGetSpace(r->blist, &len);
    if (buf == NULL) break;
    nread = recv(fd, buf, len, 0);
    if (nread == -1) {
      if (errno == EAGAIN) {
        // no data
        nread = 0;
      } else {
        // connection error
        goto ERROR;
      }
    } else if (nread == 0) {
      // connection closed
      LogInfo("connection closed");
      goto ERROR;
    }

    if (nread) {
      BufferListPush(r->blist, nread);
      SetWriteEvent(r);
      LogDebug("set write");
    } else {
      break;
    }
  }

  return;

ERROR:
  c->OnError(c);
}

void AcceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
  int cport, cfd;
  char cip[128];

  cfd = anetTcpAccept(error_, fd, cip, &cport);
  if (cfd == AE_ERR) {
    LogError("Accept client connection failed: %s", error_);
    return;
  }
  LogInfo("Accepted client from %s:%d", cip, cport);

  Client *c = AllocClient(cfd);

  if (c == NULL || aeCreateFileEvent(el, cfd, AE_READABLE, ReadIncome, c) == AE_ERR) {
    LogError("Create event failed");
    FreeClient(c);
  }
}

int main(int argc, char **argv) {
  int i, listen_fd;
  struct sigaction sig_action;

  ParseArgs(argc, argv);

  if (run_daemonize) Daemonize();

  sig_action.sa_handler = SignalHandler;
  sig_action.sa_flags = SA_RESTART;
  sigemptyset(&sig_action.sa_mask);
  sigaction(SIGINT, &sig_action, NULL);
  sigaction(SIGTERM, &sig_action, NULL);
  sigaction(SIGPIPE, &sig_action, NULL);

  if ((policy->listen.addr == NULL) || !strcmp(policy->listen.addr, "any")) {
    free(policy->listen.addr);
    policy->listen.addr = strdup("0.0.0.0");
  } else if (!strcmp(policy->listen.addr, "localhost")) {
    free(policy->listen.addr);
    policy->listen.addr = strdup("127.0.0.1");
  }

  listen_fd = anetTcpServer(error_, policy->listen.port, policy->listen.addr);

  el = aeCreateEventLoop(65536);

  if (listen_fd < 0 || aeCreateFileEvent(el, listen_fd, AE_READABLE, AcceptTcpHandler, NULL) == AE_ERR) {
    LogFatal("listen failed: %s", strerror(errno));
  }

  LogInfo("listenning on %s:%d", (policy->listen.addr? policy->listen.addr : "any"), policy->listen.port);
  for (i = 0; i < policy->nhost; i++) {
    if (policy->hosts[i].addr == NULL) policy->hosts[i].addr = strdup("127.0.0.1");
    LogInfo("proxy to %s:%d", policy->hosts[i].addr, policy->hosts[i].port);
  }

  aeMain(el);

  aeDeleteEventLoop(el);

  FreePolicy(policy);

  return 0;
}
