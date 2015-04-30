-module(mets_aggregator_count).
-behaviour(mets_aggregator).

-export([init/0,
         accumulate/2,
         emit/1,
         value/1]).

init() ->
    0.

accumulate(_Value, N) ->
    N+1.

emit(N) ->
    N.

value(N) ->
    N.
