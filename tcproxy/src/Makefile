PROGNAME   = tcproxy

OBJS   = tcproxy.o ae.o util.o policy.o zmalloc.o anet.o

CFLAGS_GEN = -Wall -Werror -g $(CFLAGS)
CFLAGS_DBG = -ggdb $(CFLAGS_GEN)
CFLAGS_OPT = -O3 -Wno-format $(CFLAGS_GEN)
DEBUG ?=

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR);
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR);

LDFLAGS   += 
LIBS      += 

all: $(PROGNAME)

%.o: %.c
	$(QUIET_CC)$(CC) -c $(CFLAGS) $(CFLAGS_OPT) $(DEBUG) $(COMPILE_TIME) $<

$(PROGNAME): $(OBJS)
	$(QUIET_LINK)$(CC) -o $(PROGNAME) $(CFLAGS_OPT) $(DEBUG) $(OBJS) $(CCLINK)
	@echo
	@echo "Make Complete. Read README for how to use."
	@echo
	@echo "Having problems with it? Send complains and bugs to dccmx@dccmx.com"
	@echo

clean:
	rm -f $(PROGNAME) core core.[1-9][0-9]* *.o memcheck.out callgrind.out.[1-9][0-9]* massif.out.[1-9][0-9]*
