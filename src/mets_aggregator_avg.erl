-module(mets_aggregator_avg).
-behaviour(mets_aggregator).

-export([init/0,
         accumulate/2,
         emit/1,
         value/1]).

-record(state,
        {avg = 0    :: number(),
         n   = 0    :: number()}).

init() ->
    #state{}.

accumulate(Value, #state{avg=Avg0, n=N0}=St) ->
    N = N0 + 1,
    Diff = Value - Avg0,
    Avg = (Diff / N) + Avg0,
    St#state{avg=Avg, n=N}.

emit(#state{avg=Avg, n=N}) ->
    {Avg, N}.

value({Avg, _N}) ->
    Avg.
