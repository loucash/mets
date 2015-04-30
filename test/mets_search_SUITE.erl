-module(mets_search_SUITE).
-author('Lukasz Biedrycki <lukasz.biedrycki@gmail.com>').

-export([all/0, suite/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([t_search_options_prop/1,
         t_search_missing_required_options_prop/1]).

-include("mets.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(th, test_helpers).
-define(NUMTESTS, 1000).
-define(PROPTEST(A), true = proper:quickcheck(A(),
                                              [{numtests, ?NUMTESTS},
                                               {constraint_tries, 1000}])).

suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
     t_search_options_prop,
     t_search_missing_required_options_prop
    ].

init_per_suite(Config) ->
    application:load(mets),
    TempFile = ?th:tempfile(),
    application:set_env(
      mets, mets_backend_sqlite,
      [
       {data_file, TempFile},
       {delete_tick, 10000}
      ]),
    ok = mets:start(),
    [{tmpfile, TempFile}|Config].

end_per_suite(Config) ->
    ok = mets:stop(),
    TempFile = ?config(tmpfile, Config),
    file:delete(TempFile),
    ok.

t_search_options_prop(_Config) ->
    ?PROPTEST(prop_search_options).

t_search_missing_required_options_prop(_Config) ->
    ?PROPTEST(prop_search_missing_required_options).

prop_search_options() ->
    ?FORALL(Options, options(),
           begin
               Response = mets:search(Options),
               {ok, []} = Response,
               true
           end).

prop_search_missing_required_options() ->
    ?FORALL(Options, missing_required_options(),
           begin
               Response = mets:search(Options),
               {error, _} = Response,
               true
           end).

options() ->
    ?LET([Req, Opt],
         [required_options(), optional_options()], Req ++ Opt).

missing_required_options() ->
    ?LET([Req, Opt],
         [missing_required_options_list(), optional_options()], Req ++ Opt).

required_options() ->
    fixed_list(required_options_list()).

missing_required_options_list() ->
    ?SUCHTHAT(L,
              list(oneof(required_options_list())),
              length(L) < length(required_options_list())).

optional_options() ->
    list(oneof(optional_options_list())).

required_options_list() ->
    [
     ns(),
     name(),
     start_time(),
     aggregate()
    ].

optional_options_list() ->
    [
     end_time(),
     tags(),
     order()
    ].

ns() ->
    {ns, non_empty(utf8_bin())}.

name() ->
    {name, non_empty(utf8_bin())}.

start_time() ->
    {start_time, integer(0, 1000000)}.

aggregate() ->
    {aggregate, {stat(), oneof([pos_integer(), {pos_integer(), units()}])}}.

stat() ->
    oneof([avg, sum, min, max, count]).

units() ->
    oneof([seconds, minutes, hours, days, weeks]).

end_time() ->
    {end_time, oneof([undefined, integer(1000000, 10000000)])}.

tags() ->
    {tags, list(?LET({K,V}, {utf8_bin(), utf8_bin()}, {K,V}))}.

order() ->
    {order, oneof([asc, desc])}.

utf8_bin() ->
    ?LET(S,
         non_empty(list(oneof([integer(16#30, 16#39),
                               integer(16#41, 16#5A),
                               integer(16#61, 16#7A)]))),
         list_to_binary(S)).
