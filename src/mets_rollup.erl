-module(mets_rollup).
-behaviour(gen_server).

%% API
-export([push/7]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

push(Aggregate, Ns, Name, Tags, Ts, Value, Options) ->
    do_push(Ns, Name, Tags, Ts, Value, Aggregate, Options).

create(Ns, Name, Tags, Ts, Aggregate, Options) ->
    gen_server:call(?MODULE, {create, Ns, Name, Tags, Ts, Aggregate, Options}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({create, Ns, Name, Tags, Ts, Aggregate, Options}, _From, State) ->
    Key = mets_utils:aggregate_key(Ns, Name, Tags, Ts, Aggregate),
    case find_worker_process(Key) of
        {ok, _} = Ok ->
            {reply, Ok, State};
        {error, not_found} ->
            {ok, Pid} = mets_rollup_sup:start_child([Ns, Name, Tags, Ts, Aggregate, Options]),
            ok = pg2:create(Key),
            ok = pg2:join(Key, Pid),
            {reply, {ok, Pid}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_push(Ns, Name, Tags, Ts, Value, {_Fun, Precision}=Aggregate, Options) ->
    AlignedTs       = mets_utils:floor(Ts, Precision),
    SanitizedTags   = mets_utils:sanitize_tags(Tags),
    Key             = mets_utils:aggregate_key(Ns, Name, SanitizedTags,
                                               AlignedTs, Aggregate),
    {ok, Pid} = case find_worker_process(Key) of
                    {ok, _} = Ok ->
                        Ok;
                    {error, not_found} ->
                        create(Ns, Name, SanitizedTags, AlignedTs, Aggregate, Options)
                end,
    mets_rollup_wrk:push(Pid, Value).

find_worker_process(Key) ->
    case pg2:get_local_members(Key) of
        [] ->
            {error, not_found};
        [Pid] ->
            {ok, Pid};
        {error, {no_such_group, _}} ->
            {error, not_found}
    end.
