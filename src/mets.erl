-module(mets).

-export([start/0, stop/0]).
-export([push/4, push/5, search/1]).
-export([get_env/1, get_env/2]).
-export([get_backend_env/2, get_backend_env/3]).

-include("mets.hrl").
-include("mets_search.hrl").

-define(APP, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    reltool_util:application_start(mets).

stop() ->
    reltool_util:application_stop(mets).

-spec push(ns(), name(), milliseconds(), value()) -> ok.
push(Ns, Name, Ts, Value) when is_binary(Ns),
                               is_binary(Name),
                               is_integer(Ts),
                               is_number(Value) ->
    push(Ns, Name, [], Ts, Value).

-spec push(ns(), name(), tags(), milliseconds(), value()) -> ok.
push(Ns, Name, Tags, Ts, Value) when is_binary(Ns),
                                     is_binary(Name),
                                     is_list(Tags),
                                     is_integer(Ts),
                                     is_number(Value) ->
    lists:foreach(
      fun({Aggregate, Options}) ->
              mets_rollup:push(Aggregate, Ns, Name, Tags, Ts, Value, Options)
      end, mets_config:match(Ns, Name, Tags)),
    ok.

-spec search(options()) -> {ok, list()} | {error, any()}.
search(Options) ->
    case verify_options(Options, #search{}) of
        {ok, Search} ->
            do_search(Search);
        {error, _} = Error ->
            Error
    end.

-spec get_env(atom()) -> term().
get_env(Name) ->
    application:get_env(?APP, Name).

-spec get_env(atom(), any()) -> term().
get_env(Name, Default) ->
    application:get_env(?APP, Name, Default).

-spec get_backend_env(atom(), atom()) -> term().
get_backend_env(Backend, Name) ->
    {ok, BackendOpts} = mets:get_env(Backend),
    proplists:get_value(Name, BackendOpts).

-spec get_backend_env(atom(), atom(), term()) -> term().
get_backend_env(Backend, Name, Default) ->
    {ok, BackendOpts} = mets:get_env(Backend),
    proplists:get_value(Name, BackendOpts, Default).

%%%===================================================================
%%% Internal
%%%===================================================================
-spec verify_options(options(), search()) -> {ok, search()} | {error, any()}.
verify_options([], Search) ->
    {ok, Search};
verify_options([{ns, Ns}|Options], Search) when is_binary(Ns) ->
    verify_options(Options, Search#search{ns=Ns});
verify_options([{name, Name}|Options], Search) when is_binary(Name) ->
    verify_options(Options, Search#search{name=Name});
verify_options([{start_time, Start}|Options], Search) when is_integer(Start) ->
    verify_options(Options, Search#search{start_time=Start});
verify_options([{end_time, End}|Options], Search) when is_integer(End) ->
    verify_options(Options, Search#search{end_time=End});
verify_options([{end_time, undefined}|Options], Search) ->
    verify_options(Options, Search);
verify_options([{tags, List}|Options], Search) when is_list(List) ->
    verify_options(Options, Search#search{tags=List});
verify_options([{order, Order}|Options], Search) when Order =:= asc orelse Order =:= desc ->
    verify_options(Options, Search#search{order=Order});
verify_options([{aggregate, {Fun, {Val, Type}}}|Options], Search) when is_integer(Val) ->
    case lists:member(Fun, ?STATS) of
        true ->
            case lists:member(Type, ?UNITS) of
                true ->
                    verify_options(Options, Search#search{source={Fun, {Val, Type}}});
                false ->
                    {error, bad_precision}
            end;
        false ->
            {error, bad_aggregate}
    end;
verify_options([{aggregate, {Fun, Val}}|Options], Search) when is_integer(Val) ->
    case lists:member(Fun, ?STATS) of
        true ->
            verify_options(Options, Search#search{source={Fun, Val}});
        false ->
            {error, bad_aggregate}
    end;
verify_options([Opt|_], _Search) ->
    {error, {bad_option, Opt}}.

do_search(#search{ns=undefined}) ->
    {error, {missing_param, ns}};
do_search(#search{name=undefined}) ->
    {error, {missing_param, name}};
do_search(#search{start_time=undefined}) ->
    {error, {missing_param, start_time}};
do_search(#search{source=undefined}) ->
    {error, {missing_param, source}};
do_search(#search{start_time=ST, end_time=ET}) when ET =/= undefined andalso
                                                    ST > ET ->
    {error, invalid_time_range};
do_search(#search{}=S) ->
    mets_backend:search(S).
