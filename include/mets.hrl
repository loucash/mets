-type ns()              :: binary().
-type name()            :: binary().
-type tags()            :: proplists:proplist().
-type value()           :: number().
-type milliseconds()    :: non_neg_integer().
-type unit()            :: seconds | minutes | hours | days | weeks.
-type precision()       :: {non_neg_integer(), unit()} | non_neg_integer().
-type stat()            :: avg | min | max | sum | count.
-type aggregate()       :: raw | {stat(), precision()}.
-type order()           :: asc | desc.
-type aggregate_state() :: term().
-type ttl()             :: infinity | pos_integer().
-type backend_options() :: [{ttl, ttl()}].

-type option()  :: {ns, ns()} |
                   {name, name()} |
                   {start_time, milliseconds()} |
                   {end_time, milliseconds()} |
                   {aggregate, aggregate()} |
                   {tags, proplists:proplist()} |
                   {order, order()}.
-type options() :: [option()].

-define(STATS, [avg, min, max, sum, count]).
-define(UNITS, [seconds, minutes, hours, days, weeks]).
