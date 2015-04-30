-record(search, {
          ns               :: ns(),
          name             :: name(),
          start_time       :: milliseconds(),
          end_time         :: milliseconds() | undefined,
          source           :: aggregate(),
          tags             :: proplists:proplist(),
          order  = asc     :: order()
         }).
-type search()  :: #search{}.
