CC     := gcc
CFLAGS := 		# Add CFLAGS=-DDEBUG with make to turn on debug

SRCS   := mkfs.c

OBJS   := ${SRCS:c=o}
PROGS  := ${SRCS:.c=}

.PHONY: all
all: ${PROGS} server client-app libmfs.so

${PROGS} : % : %.o Makefile
	${CC} $< -o $@ udp.c

clean:
	rm -f ${PROGS} ${OBJS}
	rm server 
	rm client-app 
	rm libmfs.so

server: server.c udp.c message.h mfs.h Makefile
	$(CC) $(CFLAGS) server.c -o server udp.c -lm

client-app: client-app.c mfs.c udp.c 
	$(CC) $(CFLAGS) client-app.c udp.c -o client-app

libmfs.so: mfs.c udp.c 
	gcc -c -Wall -Werror -fpic mfs.c 
	gcc -c -Wall -Werror -fpic udp.c 
	gcc -shared -o libmfs.so mfs.o udp.o

%.o: %.c Makefile
	${CC} ${CFLAGS} -c $<
