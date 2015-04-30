-module(mets_app).
-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================
start(_StartType, _StartArgs) ->
    ok = mets_config:init(),
    case mets_sup:start_link() of
        {ok, _} = Ok ->
            ok = mets_backend:init(),
            Ok;
        {error, _} = Error ->
            Error
    end.

stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
