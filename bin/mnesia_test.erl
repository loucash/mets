#!/usr/bin/env escript
%% -*- erlang -*-

-record(metrics, {key, value}).
-define(DEVICES, 10).
-define(METRICS, 15).
-define(INTERVAL, timer:seconds(2)).
-define(MNESIA_DIR, "/Users/lukasz/projects/github/most/stress/" ++ integer_to_list(?DEVICES)).

main(_) ->
    random:seed(now()),
    application:set_env(mnesia, dir, ?MNESIA_DIR),
    application:set_env(mnesia, dc_dump_limit, 40),
    application:set_env(mnesia, dump_log_write_threshold, 50000),
    Res = mnesia:create_schema([node()]),
    io:format("create_schema: ~p~n", [Res]),
    mnesia:start(),
    {atomic, ok} = mnesia:create_table(metrics,
                                       [{type, set},
                                        {disc_copies, [node()]},
                                        {attributes, record_info(fields, metrics)}]),
    ok=mnesia:wait_for_tables([metrics], 5000),
    lists:foreach(load_metrics(), lists:seq(1,?DEVICES)),
    io:format("mnesia info: ~p~n", [mnesia:info()]),
    ok.

load_metrics() ->
    Now = now_to_epoch_msecs(),
    Start = Now - weeks(1),
    AllTs = lists:seq(Start, Now, ?INTERVAL),
    fun(Id) ->
            DeviceId = <<"device_", Id/integer>>,
            io:format("Device: ~p~n", [Id]),
            lists:foreach(load_metric(DeviceId, AllTs), lists:seq(1, ?METRICS))
    end.

load_metric(DeviceId, AllTs) ->
    fun(Id) ->
            io:format("...metric: ~p~n", [Id]),
            MetricId = <<"metric_", Id/integer>>,
            Value = random:uniform(100),
            lists:foreach(
              fun(Ts) ->
                      F = fun() ->
                                  mnesia:dirty_write(#metrics{key={DeviceId, MetricId, Ts},
                                                        value=Value})
                          end,
                      mnesia:activity(async_dirty, F)
              end, AllTs)
    end.


now_to_epoch_msecs() ->
    Timestamp = os:timestamp(),
    timestamp_to_epoch_msecs(Timestamp).

timestamp_to_epoch_msecs({Megasecs, Secs, Microsecs}) ->
    Megasecs * 1000000000 + Secs * 1000 + Microsecs div 1000.

weeks(N) ->
    N * days(7).

days(N) ->
    N * timer:hours(24).
