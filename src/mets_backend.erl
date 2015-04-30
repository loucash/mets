-module(mets_backend).

%% API
-export([init/0,
         fetch/5,
         push/7,
         search/1]).

-include("mets.hrl").
-include("mets_search.hrl").

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,0},
     {fetch,5},
     {push,7},
     {search,1}];
behaviour_info(_) ->
    undefined.

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> ok.
init() ->
    {ok, BackendMod} = mets:get_env(backend),
    BackendMod:init().

-spec fetch(ns(), name(), tags(), milliseconds(), aggregate()) ->
    {ok, any()} | {error, not_found}.
fetch(Ns, Name, Tags, Ts, Aggregate) ->
    {ok, BackendMod} = mets:get_env(backend),
    BackendMod:fetch(Ns, Name, Tags, Ts, Aggregate).

-spec push(ns(), name(), tags(), milliseconds(), aggregate(), aggregate_state(), backend_options()) ->
    ok | {error, any()}.
push(Ns, Name, Tags, Ts, Aggregate, AggrState, Options) ->
    {ok, BackendMod} = mets:get_env(backend),
    BackendMod:push(Ns, Name, Tags, Ts, Aggregate, AggrState, Options).

-spec search(search()) -> {ok, list()} | {error, any()}.
search(#search{}=S) ->
    {ok, BackendMod} = mets:get_env(backend),
    BackendMod:search(S).
