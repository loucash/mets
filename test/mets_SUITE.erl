-module(mets_SUITE).
-author('Lukasz Biedrycki <lukasz.biedrycki@gmail.com>').

-export([all/0, suite/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([t_avg/1, t_min/1, t_max/1, t_sum/1, t_count/1, t_delete/1]).

-include("mets.hrl").
-include_lib("common_test/include/ct.hrl").

-define(th, test_helpers).
-define(NUMTESTS, 1000).
-define(PROPTEST(A), true = proper:quickcheck(A(),
                                              [{numtests, ?NUMTESTS},
                                               {constraint_tries, 1000}])).
-define(FOR(N, Body),
        lists:foreach(fun(I) -> Body end, lists:seq(0, N-1))).

suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
     t_avg,
     t_min,
     t_max,
     t_sum,
     t_count,
     t_delete
    ].

init_per_suite(Config) ->
    application:load(mets),
    TempFile = ?th:tempfile(),
    application:set_env(
      mets, mets_backend_sqlite,
      [
       {data_file, TempFile},
       {delete_tick, 1000}
      ]),
    application:set_env(mets, rollups_emit_tick, 100),
    application:set_env(mets, rollups_procs_ttl, 100),
    application:set_env(
      mets, rollups,
      [
        {<<"avg">>, <<"avg">>, '_', {avg, {15, minutes}}, []},
        {<<"min">>, <<"min">>, '_', {min, {15, minutes}}, []},
        {<<"max">>, <<"max">>, '_', {max, {15, minutes}}, []},
        {<<"sum">>, <<"sum">>, '_', {sum, {15, minutes}}, []},
        {<<"count">>, <<"count">>, '_', {count, {15, minutes}}, []},
        {<<"delete">>, <<"delete">>, '_', {count, {15, minutes}}, [{ttl, 1}]}
      ]
     ),
    ok = mets:start(),
    [{tmpfile, TempFile}|Config].

end_per_suite(Config) ->
    ok = mets:stop(),
    TempFile = ?config(tmpfile, Config),
    file:delete(TempFile),
    ok.

t_avg(_Config) ->
    aggregate_test(<<"avg">>, 2.5, {avg, {15, minutes}}).

t_min(_Config) ->
    aggregate_test(<<"min">>, 1, {min, {15, minutes}}).

t_max(_Config) ->
    aggregate_test(<<"max">>, 4, {max, {15, minutes}}).

t_sum(_Config) ->
    aggregate_test(<<"sum">>, 10, {sum, {15, minutes}}).

t_count(_Config) ->
    aggregate_test(<<"count">>, 4, {count, {15, minutes}}).

t_delete(_Config) ->
    NsName = <<"delete">>, Precision = {15, minutes}, Aggregate = {count, Precision},
    NowMs = mets_utils:now_to_epoch_msecs(),
    Ts = mets_utils:floor(NowMs, Precision),
    ?FOR(4, ok = mets:push(NsName, NsName, Ts+I, I+1)),
    ?th:keep_trying({ok, [ [{tags, []}, {ts, Ts}, {value, 4}] ]},
                    fun() ->
                            mets:search([{ns, NsName}, {name, NsName},
                                         {start_time, Ts}, {aggregate, Aggregate}])
                    end,
                    200, 10),
    ?th:keep_trying({ok, []},
                    fun() ->
                            mets:search([{ns, NsName},
                                         {name, NsName},
                                         {start_time, Ts},
                                         {aggregate, Aggregate}])
                    end,
                    200, 20),
    ok.

aggregate_test(NsName, Value, {_Fun, Precision}=Aggregate) ->
    NowMs = mets_utils:now_to_epoch_msecs(),
    Ts = mets_utils:floor(NowMs, Precision),
    ?FOR(4, ok = mets:push(NsName, NsName, Ts+I, I+1)),
    ?th:keep_trying({ok, [ [{tags, []}, {ts, Ts}, {value, Value}] ]},
                    fun() ->
                            mets:search([{ns, NsName}, {name, NsName},
                                         {start_time, Ts}, {aggregate, Aggregate}])
                    end,
                    200, 10),
    ok.
