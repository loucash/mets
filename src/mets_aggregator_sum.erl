-module(mets_aggregator_sum).
-behaviour(mets_aggregator).

-export([init/0,
         accumulate/2,
         emit/1,
         value/1]).

init() ->
    0.

accumulate(Value, N) ->
    N + Value.

emit(Value) ->
    Value.

value(Value) ->
    Value.
