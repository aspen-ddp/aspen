CC ?= gcc
CFLAGS ?= -O2 -g -Wall -Wextra -Werror -std=gnu11 -fPIC -pthread

TCMU_SRC ?= ../../submodules/tcmu-runner
INCLUDES = -I$(TCMU_SRC) -I$(TCMU_SRC)/ccan

TCMU_RUNNER ?= /usr/bin/tcmu-runner
LIBTCMU ?= /usr/lib/x86_64-linux-gnu/libtcmu.so.2

all: test_proto handler_aspen.so

aspen_proto.o: aspen_proto.c aspen_proto.h
	$(CC) $(CFLAGS) -c -o $@ aspen_proto.c

test_proto: test_proto.c aspen_proto.o aspen_proto.h
	$(CC) $(CFLAGS) -o $@ test_proto.c aspen_proto.o

# The handler's tcmu symbols resolve at dlopen time against tcmu-runner and libtcmu.
handler_aspen.so: handler_aspen.c aspen_proto.c aspen_proto.h
	$(CC) $(CFLAGS) $(INCLUDES) -shared -o $@ handler_aspen.c aspen_proto.c

test_handler: test_handler.c handler_aspen.c tcmu_stub.c tcmu_stub.h aspen_proto.o aspen_proto.h
	$(CC) $(CFLAGS) $(INCLUDES) -DASPEN_TEST -o $@ test_handler.c handler_aspen.c tcmu_stub.c \
		aspen_proto.o

check: test_proto test_handler
	./test_proto
	./test_handler

# Every undefined symbol must come from glibc, tcmu-runner's dynamic exports or libtcmu.
symcheck: handler_aspen.so
	@nm -D --defined-only $(TCMU_RUNNER) $(LIBTCMU) | awk 'NF == 3 { print $$3 }' | sort -u > .provided
	@nm -D --undefined-only handler_aspen.so | awk '{ print $$2 }' \
		| grep -v -E '@(GLIBC|GLIBC_PRIVATE)_' | grep -v -E '^(__gmon_start__|_ITM_.*|__cxa_finalize)$$' \
		| sed 's/@.*//' | sort -u > .needed
	@missing=$$(grep -vxF -f .provided .needed); rm -f .provided .needed; \
	if [ -n "$$missing" ]; then echo "symcheck: unresolved symbols:"; echo "$$missing"; exit 1; fi; \
	echo "symcheck: ok"

install: handler_aspen.so
	install -m 0755 handler_aspen.so $(DESTDIR)/usr/lib/x86_64-linux-gnu/tcmu-runner/

clean:
	rm -f *.o test_proto test_handler handler_aspen.so .provided .needed

.PHONY: all check symcheck install clean
