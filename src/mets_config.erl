-module(mets_config).

-export([init/0, match/3]).
-export([compile/1, do_match/4]).

init() ->
    _ = ets:new(?MODULE, [named_table, protected, {read_concurrency, true}]),
    reload_config().

match(Ns, Name, Tags) ->
    case ets:lookup(?MODULE, cfg) of
        [] -> [];
        [{cfg, Dispatch}] ->
            do_match(Dispatch, Ns, Name, Tags)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_match(Dispatch, Ns, Name, Tags) ->
    lists:usort(lists:filtermap(fun(F) -> F(Ns, Name, Tags) end, Dispatch)).

reload_config() ->
    RollupCfg = mets:get_env(rollups, []),
    Dispatch = compile(RollupCfg),
    true = ets:insert(?MODULE, {cfg, Dispatch}),
    ok.

compile(Rules) ->
    lists:map(fun compile_rule/1, Rules).

compile_rule({Ns1, Name1, Tags1, Aggregate, Options0}) ->
    NsFn   = compile_metric(Ns1),
    NameFn = compile_metric(Name1),
    TagFns = compile_tags(Tags1),
    Options = validate_options(Options0),
    fun(Ns2, Name2, Tags2) ->
        case matches_metric(Ns2, NsFn) andalso
             matches_metric(Name2, NameFn) andalso
             matches_tags(Tags2, TagFns) of
            true ->
                {true, {Aggregate, Options}};
            false ->
                false
        end
    end.

compile_metric('_') ->
    fun(_) -> true end;
compile_metric(M1) ->
    fun(M2) when M1 =:= M2 -> true;
       ({_, M2}) when M1 =:= M2 -> true;
       (_) -> false
    end.

compile_tags('_') ->
    [];
compile_tags(Tags) ->
    lists:map(fun compile_tag/1, Tags).

compile_tag({'_', '_'}) ->
    fun({_,_}) -> true end;
compile_tag({'_', V1}) ->
    fun({_, V2}) when V1 =:= V2 -> true;
       ({_, _}) -> false
    end;
compile_tag({N1, '_'}) ->
    fun({N2, _}) when N1 =:= N2 -> true;
       ({_, _}) -> false
    end;
compile_tag({N1, V1}) ->
    fun({N2, V2}) when N1 =:= N2 andalso V1 =:= V2 -> true;
       ({_, _}) -> false
    end.

matches_metric(Metric, Fn) ->
    Fn(Metric).

matches_tags(_Tags, []) -> true;
matches_tags(Tags, Fns) ->
    lists:all(fun(F) -> lists:any(F, Tags) end, Fns).

validate_options(Options) ->
    validate_options(Options, []).

validate_options([], Validated) ->
    Validated;
validate_options([{ttl, TTL}|Rest], Validated) when is_integer(TTL) ->
    validate_options(Rest, [{ttl, TTL}|Validated]);
validate_options([Opt|_], _Validated) ->
    throw({bad_option, Opt}).
