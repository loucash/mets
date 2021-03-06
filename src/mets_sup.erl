-module(mets_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Type, Args), {Id, {Id, start_link, Args},
                                permanent, 5000, Type, [Id]}).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Spec) ->
    supervisor:start_child(?MODULE, Spec).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    {ok, {{one_for_one, 5, 10},
          [?CHILD(mets_rollup_topsup, supervisor, [])]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
