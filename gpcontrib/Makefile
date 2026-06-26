default: all

top_builddir = ..
include $(top_builddir)/src/Makefile.global

.PHONY: all install clean distclean

recurse_targets = gp_internal_tools

$(call recurse,all install clean distclean, $(recurse_targets))

