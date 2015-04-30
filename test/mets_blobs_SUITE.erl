-module(mets_blobs_SUITE).
-author('Lukasz Biedrycki <lukasz.biedrycki@gmail.com>').

-export([all/0, suite/0]).
-export([t_encode_decode_rowkey/1,
         t_encode_decode_offset/1,
         t_encode_decode_timestamp/1,
         t_encode_decode_aggregate_state/1]).

-include("mets.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 1000).
-define(PROPTEST(A), true = proper:quickcheck(A(),
                                              [{numtests, ?NUMTESTS},
                                               {constraint_tries, 1000}])).


suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
        t_encode_decode_rowkey,
        t_encode_decode_offset,
        t_encode_decode_timestamp,
        t_encode_decode_aggregate_state
    ].

%% Test codec symmetrically
t_encode_decode_rowkey(_Config) ->
    ?PROPTEST(prop_encode_decode_rowkey).

t_encode_decode_offset(_Config) ->
    ?PROPTEST(prop_encode_decode_offset).

t_encode_decode_timestamp(_Config) ->
    ?PROPTEST(prop_encode_decode_timestamp).

t_encode_decode_aggregate_state(_Config) ->
    ?PROPTEST(prop_encode_decode_aggregate_state).

%% PropEr
prop_encode_decode_rowkey() ->
    ?FORALL({Ns, Name, Ts, Tags0, {Fun, Precision}=Aggregate},
            {ns(), name(), timestamp(), tags(), aggregate()},
           begin
               Tags = mets_utils:sanitize_tags(Tags0),
               PrecisionMs = mets_utils:ms(Precision),
               {ok, Bin} = mets_blobs:encode_rowkey(Ns, Name, Ts, Tags, Aggregate),
               {ok, Result} = mets_blobs:decode_rowkey(Bin),
               [{ns,        Ns},
                {name,      Name},
                {row_time,  _},
                {tags,      Tags},
                {aggregate, {Fun, PrecisionMs}}] = Result,
               true
           end).

prop_encode_decode_offset() ->
    ?FORALL(Offset, timestamp(),
           begin
               {ok, Bin} = mets_blobs:encode_offset(Offset),
               {ok, Offset} = mets_blobs:decode_offset(Bin),
               true
           end).

prop_encode_decode_timestamp() ->
    ?FORALL({Ts, Aggregate}, {timestamp(), aggregate()},
           begin
               {ok, Bin} = mets_blobs:encode_timestamp(Ts, Aggregate),
               RowTime   = mets_blobs:calculate_row_time(Ts, Aggregate),
               {ok, Ts}  = mets_blobs:decode_timestamp(Bin, RowTime),
               true
           end).

prop_encode_decode_aggregate_state() ->
    ?FORALL(AggrState, oneof([number(), {number(), number()}]),
           begin
               {ok, Bin} = mets_blobs:encode_aggregate_state(AggrState),
               {ok, AggrState} = mets_blobs:decode_aggregate_state(Bin),
               true
           end).

ns()   -> non_empty(utf8_bin()).
name() -> non_empty(utf8_bin()).

timestamp() ->
    proper_types:non_neg_integer().

tags() ->
    list(?LET({K,V}, {utf8_bin(), utf8_bin()}, {K,V})).

aggregate() ->
    ?LET({F, P}, {stat(), {pos_integer(), units()}}, {F, P}).

stat() ->
    oneof([avg, sum, min, max, count]).

units() ->
    oneof([seconds, minutes, hours, days, weeks]).

utf8_bin() ->
    ?LET(S,
         non_empty(list(oneof([integer(16#30, 16#39),
                               integer(16#41, 16#5A),
                               integer(16#61, 16#7A)]))),
         list_to_binary(S)).
