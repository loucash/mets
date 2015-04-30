-module(mets_utils).

-include("mets.hrl").

-export([aggregate_key/5,
         floor/2,
         ms/1,
         now_to_epoch_msecs/0,
         now_to_epoch_secs/0,
         row_width/1,
         sanitize_tags/1]).

-define(MAX_ROW_WIDTH, 1900000000).
-define(MAX_WEEKS, 1042).

-type epoch_milliseconds() ::
    non_neg_integer().

-type epoch_seconds() ::
    non_neg_integer().

aggregate_key(Ns, Name, Tags, Ts, {Fun, Precision}) ->
    [{<<"ns">>, Ns},
     {<<"name">>, Name},
     {<<"tags">>, Tags},
     {<<"ts">>, Ts},
     {<<"fun">>, atom_to_binary(Fun, utf8)},
     {<<"precision">>, ms(Precision)}].

-spec floor(milliseconds(), precision() | milliseconds()) ->
    milliseconds().
floor(MSec, Sample) when is_tuple(Sample) ->
    floor(MSec, ms(Sample));
floor(MSec, Sample) ->
    MSec - (MSec rem Sample).

-spec ms(precision() | milliseconds()) -> milliseconds().
ms({N, weeks}) ->
    N * ms({7, days});
ms({N, days}) ->
    N * timer:hours(24);
ms({N, Tp}) ->
    timer:Tp(N);
ms(N) when is_integer(N) ->
    N.

-spec now_to_epoch_msecs() ->
    epoch_milliseconds().
now_to_epoch_msecs() ->
    Timestamp = os:timestamp(),
    timestamp_to_epoch_msecs(Timestamp).

-spec now_to_epoch_secs() ->
    epoch_seconds().
now_to_epoch_secs() ->
    Timestamp = os:timestamp(),
    timestamp_to_epoch_secs(Timestamp).

-spec timestamp_to_epoch_msecs(erlang:timestamp()) ->
    epoch_milliseconds().
timestamp_to_epoch_msecs({Megasecs, Secs, Microsecs}) ->
    Megasecs * 1000000000 + Secs * 1000 + Microsecs div 1000.

-spec timestamp_to_epoch_secs(erlang:timestamp()) ->
    epoch_seconds().
timestamp_to_epoch_secs({Megasecs, Secs, _Microsecs}) ->
    Megasecs * 1000000 + Secs.

row_width({_, Precision}) ->
    calc_row_width(Precision).

calc_row_width(Ms) when is_tuple(Ms) ->
    calc_row_width(ms(Ms));
calc_row_width(Ms) when is_integer(Ms) ->
    Weeks = (Ms * ?MAX_ROW_WIDTH) div ms({1, weeks}),
    {lists:min([?MAX_WEEKS, Weeks]), weeks}.

sanitize_tags(Tags) ->
    FoldTagsFun =
    fun({Name, Value}, D0) when is_list(Value) ->
        case orddict:find(Name, D0) of
            error ->
                orddict:store(Name, lists:sort(Value), D0);
            {ok, List} ->
                orddict:store(Name, lists:sort(Value ++ List), D0)
        end;
       ({Name, Value}, D0) ->
        case orddict:find(Name, D0) of
            error ->
                orddict:append(Name, Value, D0);
            {ok, List} ->
                orddict:store(Name, lists:sort([Value|List]), D0)
        end
    end,
    lists:foldl(FoldTagsFun, orddict:new(), lists:usort(Tags)).
