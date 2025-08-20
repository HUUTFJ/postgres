# contrib/vci/port/Makefile

subdir = contrib/vci/port
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(PORTNAME), linux)
SUBOBJS += vci_linux.o
endif
ifeq ($(PORTNAME), win32)
SUBOBJS = vci_win32.o
endif

EXTRA_CLEAN = SUBSYS.o $(SUBOBJS)

PG_CPPFLAGS = -I$(top_srcdir)/contrib/vci/include

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
include $(top_srcdir)/contrib/contrib-global.mk
endif

override CFLAGS += $(CFLAGS_SL)
