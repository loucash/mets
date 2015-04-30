-module(mets_blobs).

-export([encode_aggregate/1,
         encode_offset/1,
         decode_offset/1,
         encode_rowkey/5,
         decode_rowkey/1,
         encode_row_time/2,
         encode_timestamp/2,
         decode_timestamp/2,
         encode_aggregate_state/1,
         decode_aggregate_state/1]).
-export([calculate_row_time/2]).

-define(MODEL_VERSION, 16#01).

-define(TYPE_LONG,   16#01).
-define(TYPE_DOUBLE, 16#02).

-define(FUN_MAX, 16#01).
-define(FUN_MIN, 16#02).
-define(FUN_AVG, 16#03).
-define(FUN_SUM, 16#04).
-define(FUN_COUNT, 16#05).

-define(NULL, 16#00).

%%%===================================================================
%%% API
%%%===================================================================
encode_aggregate({Fun, Precision}) ->
    {<<(encode_aggregate_fun(Fun)):8/integer>>,
     <<(mets_utils:ms(Precision)):64/integer>>,
     <<?NULL:64/integer>>}.

decode_aggregate(Fun, Precision, _) ->
    {decode_aggregate_fun(Fun), Precision}.

encode_offset(Offset) ->
    {ok, <<Offset:64/integer>>}.

decode_offset(<<Offset:64/integer>>) ->
    {ok, Offset}.

encode_rowkey(Ns, Name, Ts, Tags, {Fun, Precision}) when is_tuple(Precision) ->
    encode_rowkey(Ns, Name, Ts, Tags, {Fun, mets_utils:ms(Precision)});
encode_rowkey(Ns, Name, Ts, Tags0, {_, Precision}=Aggregate) when is_integer(Precision) andalso
                                                                  Precision > 0 ->
    RowTime    = encode_row_time(Ts, Aggregate),
    NsLength   = byte_size(Ns),
    NameLength = byte_size(Name),
    {AggregateFun,
     AggregateParam1,
     AggregateParam2} = encode_aggregate(Aggregate),
    Tags             = encode_tags(Tags0),
    TagsLength       = byte_size(Tags),

    Bin = <<?MODEL_VERSION:8/integer,
            NsLength:8/integer,
            Ns/binary,
            NameLength:8/integer,
            Name/binary,
            AggregateFun/binary,
            AggregateParam1/binary,
            AggregateParam2/binary,
            RowTime/binary,
            TagsLength:16/integer,
            Tags/binary>>,
    {ok, Bin}.

-spec decode_rowkey(binary()) -> {ok, proplists:proplist()} | {error, invalid}.
decode_rowkey(<<?MODEL_VERSION:8/integer,
                NsLength:8/integer,
                Ns:NsLength/binary-unit:8,
                NameLength:8/integer,
                Name:NameLength/binary-unit:8,
                AggregateFun:8/integer,
                AggregateParam1:64/integer,
                AggregateParam2:64/integer,
                RowTime:64/integer,
                TagsLength:16/integer,
                Tags:TagsLength/binary-unit:8
              >>) ->
    Result = [{ns,        Ns},
              {name,      Name},
              {row_time,  RowTime},
              {tags,      decode_tags(Tags)},
              {aggregate, decode_aggregate(AggregateFun,
                                           AggregateParam1,
                                           AggregateParam2)}],
    {ok, Result};
decode_rowkey(_) ->
    {error, invalid}.

encode_row_time(Ts, Aggregate) ->
    RowTime = calculate_row_time(Ts, Aggregate),
    encode_row_time(RowTime).

encode_timestamp(Ts, Aggregate) ->
    RowTime  = calculate_row_time(Ts, Aggregate),
    Offset   = Ts - RowTime,
    encode_offset(Offset).

decode_timestamp(OffsetBin, RowTime) ->
    {ok, Offset} = decode_offset(OffsetBin),
    {ok, RowTime+Offset}.

encode_aggregate_state(AggrState) ->
    {ok, term_to_binary(AggrState, [{compressed, 9}, {minor_version, 1}])}.

decode_aggregate_state(Bin) ->
    {ok, binary_to_term(Bin)}.

calculate_row_time(Ts, Aggregate) ->
    RowWidth = mets_utils:row_width(Aggregate),
    mets_utils:floor(Ts, RowWidth).

%%%===================================================================
%%% Internal functions
%%%===================================================================
encode_row_time(RowTime) ->
    <<RowTime:64/integer>>.

encode_aggregate_fun(max) -> ?FUN_MAX;
encode_aggregate_fun(min) -> ?FUN_MIN;
encode_aggregate_fun(avg) -> ?FUN_AVG;
encode_aggregate_fun(sum) -> ?FUN_SUM;
encode_aggregate_fun(count) -> ?FUN_COUNT.

decode_aggregate_fun(?FUN_MAX) -> max;
decode_aggregate_fun(?FUN_MIN) -> min;
decode_aggregate_fun(?FUN_AVG) -> avg;
decode_aggregate_fun(?FUN_SUM) -> sum;
decode_aggregate_fun(?FUN_COUNT) -> count.

encode_tags([]) -> <<>>;
encode_tags(Tags) ->
    msgpack:pack(mets_utils:sanitize_tags(Tags), [{format,jsx}]).

decode_tags(<<>>) -> [];
decode_tags(Tags) ->
    {ok, T} = msgpack:unpack(Tags, [{format,jsx}]),
    T.
