-module(mets_rollup_wrk).
-behaviour(gen_fsm).

%% API
-export([push/2]).
-export([start_link/6]).

%% states
-export([active/2,
         inactive/2,
         prepare/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {
          ns,
          name,
          tags,
          ts,
          aggregate,
          aggregate_mod,
          aggregate_st,
          options,
          timer
         }).

% how often dump data to backend
-define(EMIT_INTERVAL, timer:seconds(30)).

% max inactivity time after which stop this fsm
-define(SHUTDOWN_TIMEOUT, timer:minutes(5)).

%%%===================================================================
%%% API
%%%===================================================================
push(Pid, Value) ->
    gen_fsm:send_event(Pid, {push, Value}).

start_link(Ns, Name, Tags, Ts, Aggregate, Options) ->
    gen_fsm:start_link(?MODULE, [Ns, Name, Tags, Ts, Aggregate, Options], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([Ns, Name, Tags, Ts, Aggregate, Options]) ->
    {ok, prepare, #state{ns=Ns, name=Name, tags=Tags, ts=Ts,
                         aggregate=Aggregate,
                         aggregate_mod=mets_aggregator:mod(Aggregate),
                         options=Options}, 0}.

prepare(timeout, #state{ns=Ns, name=Name, tags=Tags, ts=Ts,
                        aggregate=Aggregate,
                        aggregate_mod=Mod}=State0) ->
    State1 = case mets_backend:fetch(Ns, Name, Tags, Ts, Aggregate) of
                 {ok, _} ->
                     todo;
                 {error, not_found} ->
                     State0#state{aggregate_st=Mod:init()}
             end,
    {next_state, inactive, State1, shutdown_ttl()}.

inactive({push, Value}, State) ->
    Ref = erlang:send_after(emit_tick(), self(), emit),
    {next_state, active, accumulate(Value, State#state{timer=Ref})};
inactive(timeout, State) ->
    {stop, normal, State}.

active({push, Value}, State) ->
    {next_state, active, accumulate(Value, State)};
active(emit, #state{ns=Ns, name=Name, tags=Tags, ts=Ts, aggregate=Aggregate,
                    options=Options}=State) ->
    Result = mets_backend:push(Ns, Name, Tags, Ts, Aggregate, emit(State), Options),
    case Result of
        ok ->
            {next_state, inactive, State#state{timer=undefined}, shutdown_ttl()};
        {error, _} ->
            Ref = erlang:send_after(emit_tick(), self(), emit),
            {next_state, active, State#state{timer=Ref}}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(Info, StateName, State) ->
    ?MODULE:StateName(Info, State).

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate(Value, #state{aggregate_st=AggrSt0, aggregate_mod=Mod}=St) ->
    St#state{aggregate_st=Mod:accumulate(Value, AggrSt0)}.

emit(#state{aggregate_st=AggrSt0, aggregate_mod=Mod}) ->
    Mod:emit(AggrSt0).

emit_tick() ->
    mets:get_env(rollups_emit_tick, ?EMIT_INTERVAL).

shutdown_ttl() ->
    mets:get_env(rollups_procs_ttl, ?SHUTDOWN_TIMEOUT).
