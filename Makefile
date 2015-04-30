PROJECT := mets
REBAR := ./rebar
SNAME := $(PROJECT)

ERL := erl
EPATH = -pa ebin -pz deps/*/ebin -pa ../mets
TEST_EPATH = -pz deps/*/ebin -I deps/proper/include -pa ebin -pa test
PLT_APPS = $(shell ls $(ERL_LIB_DIR) | grep -v interface | sed -e 's/-[0-9.]*//')
DIALYZER_OPTS= -Wno_undefined_callbacks --fullpath
CQLSH = cqlsh

.PHONY: all build_plt compile configure console deps doc clean depclean distclean dialyze test

all: deps compile

build_plt:
	@dialyzer --build_plt --apps $(PLT_APPS)

compile:
	$(REBAR) compile

compile-fast:
	$(REBAR) skip_deps=true compile

configure:
	$(REBAR) get-deps compile

console:
	$(ERL) -sname $(PROJECT) $(EPATH)

deps:
	$(REBAR) get-deps

doc:
	$(REBAR) skip_deps=true doc

clean:
	$(REBAR) skip_deps=true clean

depclean:
	$(REBAR) clean

distclean:
	$(REBAR) clean delete-deps
	@rm -rf logs
	@rm -rf ct_log
	@rm -rf log

dialyze:
	@dialyzer $(DIALYZER_OPTS) -r ebin

dialyze/dialyzer_plt:
	mkdir -p dialyze
	curl -L "https://github.com/esl/erlang-plts/blob/master/plts/travis-erlang-r16b02.plt?raw=true" -o dialyze/dialyzer_plt
	cp dialyze/dialyzer_plt /home/travis/.dialyzer_plt

/home/travis/.dialyzer_plt:
	cp dialyze/dialyzer_plt /home/travis/.dialyzer_plt

dialyzer-travis: dialyze/dialyzer_plt /home/travis/.dialyzer_plt
	@dialyzer $(DIALYZER_OPTS) -r ebin

start:
	$(ERL) -sname $(PROJECT) $(EPATH) -s $(PROJECT)

test: deps compile
	$(REBAR) -C rebar.test.config get-deps compile
	$(REBAR) -C rebar.test.config ct skip_deps=true

test-console: test-compile
	@erlc $(TEST_EPATH) -o test test/*.erl
	$(ERL) -sname $(PROJECT)_test  $(TEST_EPATH) -config sys
