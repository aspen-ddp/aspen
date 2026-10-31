CC ?= gcc
CFLAGS ?= -O2 -g -Wall -Wextra -Werror -std=gnu11 -fPIC -pthread

# handler_aspen.so is added to `all` in a later task.
all: test_proto

aspen_proto.o: aspen_proto.c aspen_proto.h
	$(CC) $(CFLAGS) -c -o $@ aspen_proto.c

test_proto: test_proto.c aspen_proto.o aspen_proto.h
	$(CC) $(CFLAGS) -o $@ test_proto.c aspen_proto.o

check: test_proto
	./test_proto

clean:
	rm -f *.o test_proto handler_aspen.so

.PHONY: all check clean
