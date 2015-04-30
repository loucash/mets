-module(mets_aggregator).

-export([mod/1]).
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,0},
     {accumulate,2},
     {emit,1},
     {value,1}];
behaviour_info(_) ->
    undefined.


mod({avg, _}) -> mets_aggregator_avg;
mod({max, _}) -> mets_aggregator_max;
mod({min, _}) -> mets_aggregator_min;
mod({sum, _}) -> mets_aggregator_sum;
mod({count, _}) -> mets_aggregator_count.
