-module(mets_aggregator_min).
-behaviour(mets_aggregator).

-export([init/0,
         accumulate/2,
         emit/1,
         value/1]).

init() ->
    undefined.

accumulate(Value, undefined) ->
    Value;
accumulate(Value1, Value2) when Value1 > Value2 ->
    Value2;
accumulate(Value1, Value2) when Value1 =< Value2 ->
    Value1.

emit(Value) ->
    Value.

value(Value) ->
    Value.
