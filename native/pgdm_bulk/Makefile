EXTENSION = pgdm_bulk
MODULES = pgdm_bulk
DATA = pgdm_bulk--1.5.sql pgdm_bulk--1.4--1.5.sql pgdm_bulk--1.4.sql pgdm_bulk--1.3--1.4.sql pgdm_bulk--1.3.sql pgdm_bulk--1.2--1.3.sql pgdm_bulk--1.2.sql pgdm_bulk--1.1--1.2.sql pgdm_bulk--1.0.sql pgdm_bulk--1.1.sql pgdm_bulk--1.0--1.1.sql
PG_CONFIG ?= pg_config
SOURCE_SHA256 := $(shell bash build_id.sh --hash)
PG_CPPFLAGS += -DPGDM_BULK_SOURCE_SHA256=\"$(SOURCE_SHA256)\"
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

pgdm_bulk.o pgdm_bulk.bc: $(wildcard *.c *.h *.inc) build_id.sh Makefile
