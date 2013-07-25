AR = ar
CC = gcc
CXX = c++ -std=gnu++0x
CFLAGS = -g -W -Wall -fPIC
# -O2
DEPSDIR := .deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
ifeq ($(strip $(MEMMGR)), )
  MEMMGR = 
endif
ifneq ($(strip $(KEYSWAP)), )
  CFLAGS += -DKEYSWAP
endif
ifneq ($(strip $(NOPREFETCH)), )
  CFLAGS += -DNOPREFETCH
endif
ifneq ($(strip $(NOSUPERPAGE)), )
  CFLAGS += -DNOSUPERPAGE
endif
LIBS =  -lpthread -lm
LDFLAGS = 

all: libmtree.a libmtree.dylib asr_mttest

%.o: %.c config.h $(DEPSDIR)/stamp
	$(CXX) $(CFLAGS) $(DEPCFLAGS) -include config.h -c -o $@ $<

%.o: %.cc config.h $(DEPSDIR)/stamp
	$(CXX) $(CFLAGS) $(DEPCFLAGS) -include config.h -c -o $@ $<

libjson.a: json.o string.o straccum.o hashallocator.o clp.o kvrandom.o compiler.o kvthread.o str.o
	@/bin/rm -f $@
	$(AR) cru $@ $^

KVTREES = masstree.o \
	value_string.o value_array.o value_versioned_array.o perfstat.o \
	string_slice.o

mtd: mtd.o log.o checkpoint.o file.o misc.o $(KVTREES) \
	kvio.o libjson.a
	$(CXX) $(CFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

mtclient: mtclient.o misc.o kvio.o libjson.a
	$(CXX) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

mttest: mttest.o misc.o checkpoint.o $(KVTREES) \
	kvio.o libjson.a
	$(CXX) $(CFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

test_string: test_string.o string.o straccum.o compiler.o
	$(CXX) $(CFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

test_atomics: test_atomics.o string.o straccum.o kvrandom.o \
	json.o compiler.o kvio.o
	$(CXX) $(CFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

#ASR:
libmtree.a: js_mt_shim.o string.o straccum.o hashallocator.o clp.o kvrandom.o compiler.o kvthread.o str.o checkpoint.o file.o misc.o kvio.o $(KVTREES)
	@/bin/rm -f $@
	$(AR) cru $@ $^

libmtree.dylib: js_mt_shim.o string.o straccum.o hashallocator.o clp.o kvrandom.o compiler.o kvthread.o str.o checkpoint.o file.o misc.o kvio.o $(KVTREES)
	$(CXX) $(CFLAGS) -dynamiclib -o libmtree.dylib $^

asr_mttest: asr_mttest.o libmtree.a
	$(CXX) $(CFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)	

config.status: configure
	./configure  'CXX=c++ -std=gnu++0x -stdlib=libc++'

$(DEPSDIR)/stamp:
	mkdir -p $(DEPSDIR)
	touch $@

stamp-h: config.h.in config.status
	CONFIG_FILES= $(SHELL) ./config.status
	echo > stamp-h

clean:
	rm -f mtd mtclient mttest test_string test_atomics *.o libjson.a
	rm -rf .deps

DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif

.PHONY: clean all
