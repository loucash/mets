-module(mets_rollup_topsup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    Router = {mets_rollup,
              {mets_rollup, start_link, []},
              permanent, infinity, worker, [mets_rollup]},
    Workers = {mets_rollup_sup,
               {mets_rollup_sup, start_link, []},
               permanent, infinity, supervisor, [mets_rollup_sup]},
    {ok, {{one_for_one, 5, 10}, [Router, Workers]}}.
