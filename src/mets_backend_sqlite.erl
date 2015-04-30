-module(mets_backend_sqlite).
-behaviour(gen_server).
-behaviour(mets_backend).

-include("mets.hrl").
-include("mets_search.hrl").

%% API
-export([init/0, fetch/5, push/7, search/1]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          data_file         :: string(),
          db                :: term(),
          reads_row_size    :: pos_integer(),
          statements        :: ets:tid(),
          delete_tick       :: pos_integer()
         }).

-define(INSERT_DATA_POINT,              insert_data_point_q).
-define(INSERT_DATA_POINT_TTL,          insert_data_point_ttl_q).
-define(INSERT_ROW_INDEX,               insert_row_index_q).
-define(INSERT_ROW_INDEX_TTL,           insert_row_index_ttl_q).
-define(SELECT_DATA_POINT,              select_value_q).
-define(SELECT_ROWS_FROM_START_ASC,     select_start_row_index_asc_q).
-define(SELECT_ROWS_FROM_START_DESC,    select_start_row_index_desc_q).
-define(SELECT_ROWS_FROM_START(Order),
        (fun(asc)  -> ?SELECT_ROWS_FROM_START_ASC;
            (desc) -> ?SELECT_ROWS_FROM_START_DESC end)(Order)).
-define(SELECT_ROWS_IN_RANGE_ASC,       select_range_row_index_asc_q).
-define(SELECT_ROWS_IN_RANGE_DESC,      select_range_row_index_desc_q).
-define(SELECT_ROWS_IN_RANGE(Order),
        (fun(asc)  -> ?SELECT_ROWS_IN_RANGE_ASC;
            (desc) -> ?SELECT_ROWS_IN_RANGE_DESC end)(Order)).
-define(SELECT_DATA_FROM_START_ASC,     select_start_data_point_asc_q).
-define(SELECT_DATA_FROM_START_DESC,    select_start_data_point_desc_q).
-define(SELECT_DATA_FROM_START(Order),
        (fun(asc)  -> ?SELECT_DATA_FROM_START_ASC;
            (desc) -> ?SELECT_DATA_FROM_START_DESC end)(Order)).
-define(SELECT_DATA_IN_RANGE_ASC,       select_range_data_point_asc_q).
-define(SELECT_DATA_IN_RANGE_DESC,      select_range_data_point_desc_q).
-define(SELECT_DATA_IN_RANGE(Order),
        (fun(asc)  -> ?SELECT_DATA_IN_RANGE_ASC;
            (desc) -> ?SELECT_DATA_IN_RANGE_DESC end)(Order)).
-define(DELETE_DATA_POINT,              delete_data_point_q).
-define(DELETE_ROW_INDEX,               delete_row_index_q).

-define(DEFAULT_READS_ROW_SIZE, 10240).

%%%===================================================================
%%% API
%%%===================================================================
-spec init() -> ok.
init() ->
    Spec = {?MODULE, {?MODULE, start_link, []},
            transient, 5000, worker, [?MODULE]},
    {ok, _} = mets_sup:start_child(Spec),
    ok.

-spec fetch(ns(), name(), tags(), milliseconds(), aggregate()) -> {ok, number()}
                                                                | {error, not_found}.
fetch(Ns, Name, Tags, Ts, Aggregate) ->
    Qry = select_data_point_query(Ns, Name, Tags, Ts, Aggregate),
    case select(Qry) of
        {ok, [[Bin]]} ->
            mets_blobs:decode_aggregate_state(Bin);
        {error, _} = Error ->
            Error
    end.

-spec push(ns(), name(), tags(), milliseconds(), aggregate(), aggregate_state(),
           backend_options()) -> ok.
push(Ns, Name, Tags, Ts, Aggregate, AggrState, Options) ->
    TTL = proplists:get_value(ttl, Options, infinity),
    Qrys = insert_queries(Ns, Name, Tags, Ts, Aggregate, AggrState, TTL),
    gen_server:call(?MODULE, {insert, Qrys}).

-spec search(search()) -> {ok, list()} | {error, any()}.
search(#search{}=S) ->
    Fns = [
        fun(_)    -> search_rows(S) end,
        fun(Rows) -> decode_rowkeys(Rows) end,
        fun(Rows) -> filter_row_tags(S, Rows) end,
        fun(Rows) -> search_data_points(S, Rows) end
    ],
    hope_result:pipe(Fns, undefined).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    DataFile     = mets:get_backend_env(mets_backend_sqlite, data_file),
    DeleteTick   = mets:get_backend_env(mets_backend_sqlite, delete_tick),
    ReadsRowSize = mets:get_backend_env(mets_backend_sqlite, reads_row_size, ?DEFAULT_READS_ROW_SIZE),
    {ok, Db}     = esqlite3:open(DataFile),
    StmtTid      = ets:new(statements, []),

    State = #state{data_file=DataFile, db=Db, statements=StmtTid,
                   reads_row_size=ReadsRowSize, delete_tick=DeleteTick},
    prepare_db(),
    restart_delete_timer(State),
    {ok, State}.

handle_call({insert, Qrys}, _From, State) ->
    Response = run_in_transaction(Qrys, State),
    {reply, Response, State};
handle_call({select, Qry}, _From, State) ->
    Result = execute_query(Qry, State),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(delete_outdated, State) ->
    Now = mets_utils:now_to_epoch_secs(),
    _ = execute_query(delete_data_point(Now), State),
    _ = execute_query(delete_row_index(Now), State),
    restart_delete_timer(State),
    {noreply, State};
handle_info(prepare_db, State) ->
    ok = ensure_tables_created(State),
    ok = prepare_statements(State),
    {noreply, State}.

terminate(_Reason, #state{db=Db}) ->
    esqlite3:close(Db),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
restart_delete_timer(#state{delete_tick=DeleteTick}) ->
    erlang:send_after(DeleteTick, self(), delete_outdated).

prepare_db() ->
    self() ! prepare_db.

select(Qry) ->
    gen_server:call(?MODULE, {select, Qry}).

ensure_tables_created(#state{db=Db}) ->
    PrivDir     = code:priv_dir(mets),
    SchemaFile  = PrivDir ++ "/sqlite_schema.sql",
    {ok, Schema} = file:read_file(SchemaFile),
    esqlite3:exec(Schema, Db).

prepare_statements(#state{statements=StmtTid, db=Db}) ->
    lists:foreach(
      fun({StmtId, Stmt}) ->
        {ok, Prepared} = esqlite3:prepare(Stmt, Db),
        true = ets:insert(StmtTid, {StmtId, Prepared})
      end,
      statements()),
    ok.

statements() ->
    [{?INSERT_DATA_POINT,
      <<"INSERT INTO data_points(rowkey, offset, value) VALUES (?1, ?2, ?3);">>},
     {?INSERT_DATA_POINT_TTL,
      <<"INSERT INTO data_points VALUES (?1, ?2, ?3, ?4);">>},
     {?INSERT_ROW_INDEX,
      <<"INSERT INTO row_key_index(ns, name, aggregate_fun, aggregate_param1, "
        "aggregate_param2, rowtime, rowkey) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);">>},
     {?INSERT_ROW_INDEX_TTL,
      <<"INSERT INTO row_key_index "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);">>},
     {?SELECT_DATA_POINT, <<"SELECT value FROM data_points WHERE rowkey=?1 AND offset=?2">>},
     {?SELECT_ROWS_FROM_START_ASC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "ns = ?1 AND name = ?2 AND aggregate_fun = ?3 AND aggregate_param1 = ?4 AND "
        "aggregate_param2 = ?5 AND rowtime >= ?6 ORDER BY rowtime ASC;">>},
     {?SELECT_ROWS_FROM_START_DESC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "ns = ?1 AND name = ?2 AND aggregate_fun = ?3 AND aggregate_param1 = ?4 AND "
        "aggregate_param2 = ?5 AND rowtime >= ?6 ORDER BY rowtime DESC;">>},
     {?SELECT_ROWS_IN_RANGE_ASC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "ns = ?1 AND name = ?2 AND aggregate_fun = ?3 AND aggregate_param1 = ?4 AND "
        "aggregate_param2 = ?5 AND rowtime >= ?6 AND rowtime <= ?7 ORDER BY rowtime ASC;">>},
     {?SELECT_ROWS_IN_RANGE_DESC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "ns = ?1 AND name = ?2 AND aggregate_fun = ?3 AND aggregate_param1 = ?4 AND "
        "aggregate_param2 = ?5 AND rowtime >= ?6 AND rowtime <= ?7 ORDER BY rowtime DESC;">>},
     {?SELECT_DATA_FROM_START_ASC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ?1 AND offset >= ?2 ORDER BY offset ASC  LIMIT ?3;">>},
     {?SELECT_DATA_FROM_START_DESC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ?1 AND offset >= ?2 ORDER BY offset DESC LIMIT ?3;">>},
     {?SELECT_DATA_IN_RANGE_ASC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ?1 AND offset >= ?2 AND offset <= ?3 LIMIT ?4;">>},
     {?SELECT_DATA_IN_RANGE_DESC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ?1 AND offset >= ?2 AND offset <= ?3 ORDER BY offset DESC LIMIT ?4;">>},
     {?DELETE_DATA_POINT, <<"DELETE FROM data_points WHERE ttl < ?1">>},
     {?DELETE_ROW_INDEX, <<"DELETE FROM row_key_index WHERE ttl < ?1">>}].

insert_queries(Ns, Name, Tags, Ts, Aggregate, AggrState, TTL) ->
    {ok, RowKey} = mets_blobs:encode_rowkey(Ns, Name, Ts, Tags, Aggregate),
    Q1 = insert_data_point(RowKey, Ts, Aggregate, AggrState, TTL),
    Q2 = insert_row_index(RowKey, Ns, Name, Ts, Aggregate, TTL),
    [Q1, Q2].

insert_data_point(RowKey, Ts, Aggregate, AggrState0, infinity) ->
    {ok, Timestamp} = mets_blobs:encode_timestamp(Ts, Aggregate),
    {ok, AggrState} = mets_blobs:encode_aggregate_state(AggrState0),
    {?INSERT_DATA_POINT, [RowKey, Timestamp, AggrState]};
insert_data_point(RowKey, Ts, Aggregate, AggrState0, TTL0) when is_integer(TTL0) ->
    DeleteAt = mets_utils:now_to_epoch_secs() + TTL0,
    {ok, Timestamp} = mets_blobs:encode_timestamp(Ts, Aggregate),
    {ok, AggrState} = mets_blobs:encode_aggregate_state(AggrState0),
    {?INSERT_DATA_POINT_TTL, [RowKey, Timestamp, AggrState, DeleteAt]}.

insert_row_index(RowKey, Ns, Name, Ts, Aggregate, infinity) ->
    {Fun, Arg1, Arg2} = mets_blobs:encode_aggregate(Aggregate),
    RowTime = mets_blobs:encode_row_time(Ts, Aggregate),
    {?INSERT_ROW_INDEX, [Ns, Name, Fun, Arg1, Arg2, RowTime, RowKey]};
insert_row_index(RowKey, Ns, Name, Ts, Aggregate, TTL) when is_integer(TTL) ->
    DeleteAt = row_index_ttl(TTL, Aggregate),
    {Fun, Arg1, Arg2} = mets_blobs:encode_aggregate(Aggregate),
    RowTime = mets_blobs:encode_row_time(Ts, Aggregate),
    {?INSERT_ROW_INDEX_TTL, [Ns, Name, Fun, Arg1, Arg2, RowTime, RowKey, DeleteAt]}.

row_index_ttl(TTL, Aggregate) ->
    RowWidth    = mets_utils:row_width(Aggregate),
    RowWidthMs  = mets_utils:ms(RowWidth),
    RowWidthSeconds = RowWidthMs div 1000,
    mets_utils:now_to_epoch_secs() + RowWidthSeconds + TTL.

delete_data_point(Now) ->
    {?DELETE_DATA_POINT, [Now]}.

delete_row_index(Now) ->
    {?DELETE_ROW_INDEX, [Now]}.

run_in_transaction(Qrys, #state{db=Db, statements=StmtTid}) ->
    ok = esqlite3:exec("begin;", Db),
    lists:foreach(
      fun({StmtId, Args}) ->
            Stmt = find_statement(StmtId, StmtTid),
            esqlite3:bind(Stmt, Args),
            esqlite3:step(Stmt)
      end, Qrys),
    ok = esqlite3:exec("commit;", Db),
    ok.

execute_query({StmtId, Args}, #state{statements=StmtTid}) ->
    Stmt = find_statement(StmtId, StmtTid),
    esqlite3:bind(Stmt, Args),
    steps(Stmt, []).

steps(Stmt, []) ->
    case esqlite3:step(Stmt) of
        '$done' ->
            {error, not_found};
        {row, Row} ->
            steps(Stmt, [tuple_to_list(Row)])
    end;
steps(Stmt, Acc) ->
    case esqlite3:step(Stmt) of
        '$done' ->
            {ok, lists:reverse(Acc)};
        {row, Row} ->
            steps(Stmt, [tuple_to_list(Row)|Acc])
    end.

find_statement(StmtId, StmtTid) ->
    [{_, Stmt}] = ets:lookup(StmtTid, StmtId),
    Stmt.

select_data_point_query(Ns, Name, Tags, Ts, Aggregate) ->
    {ok, RowKey} = mets_blobs:encode_rowkey(Ns, Name, Ts, Tags, Aggregate),
    {ok, Timestamp} = mets_blobs:encode_timestamp(Ts, Aggregate),
    {?SELECT_DATA_POINT, [RowKey, Timestamp]}.

%%%===================================================================
%%% Internal search functions
%%%===================================================================
search_rows(#search{ns=Ns, name=Name, order=Order, source=Source,
                    start_time=StartTs, end_time=EndTs}) ->
    Qry = select_rows_query(Ns, Name, StartTs, EndTs, Source, Order),
    case select(Qry) of
        {ok, _} = Ok ->
            Ok;
        {error, not_found} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

select_rows_query(Ns, Name, StartTs, undefined, Source, Order) ->
    {Fun, Arg1, Arg2} = mets_blobs:encode_aggregate(Source),
    StartTsBin        = mets_blobs:encode_row_time(StartTs, Source),
    {?SELECT_ROWS_FROM_START(Order), [Ns, Name, Fun, Arg1, Arg2, StartTsBin]};
select_rows_query(Ns, Name, StartTs, EndTs, Source, Order) ->
    {Fun, Arg1, Arg2} = mets_blobs:encode_aggregate(Source),
    StartTsBin        = mets_blobs:encode_row_time(StartTs, Source),
    EndRowTime        = mets_blobs:calculate_row_time(EndTs, Source) + 1,
    EndTsBin          = mets_blobs:encode_row_time(EndRowTime, Source),
    {?SELECT_ROWS_IN_RANGE(Order), [Ns, Name, Fun, Arg1, Arg2, StartTsBin, EndTsBin]}.


decode_rowkeys(RowKeys) ->
    Rows = lists:map(
             fun([RowKey]) ->
                {ok, RowProps} = mets_blobs:decode_rowkey(RowKey),
                {RowKey, RowProps}
             end, RowKeys),
    {ok, Rows}.

filter_row_tags(#search{tags=[]}, Rows) -> {ok, Rows};
filter_row_tags(#search{tags=undefined}, Rows) -> {ok, Rows};
filter_row_tags(#search{tags=Tags}, Rows) ->
    {ok, lists:filter(match_row_fun(Tags), Rows)}.

match_row_fun(Tags) ->
    MatchTagFuns = lists:map(fun match_tag_fun/1, Tags),
    fun({_RowKey, RowProps}) ->
        RowTags = proplists:get_value(tags, RowProps),
        lists:all(matching_tags(RowTags), MatchTagFuns)
    end.

match_tag_fun({FilterKey, FilterValue}) ->
    fun({Key, Values}) when FilterKey =:= Key ->
            lists:member(FilterValue, Values);
       (_) -> false
    end.

matching_tags(RowTags) ->
    fun(MatchFun) ->
        lists:any(MatchFun, RowTags)
    end.

search_data_points(#search{}=S, Rows) ->
    Results0 = lists:map(fun(Row) -> do_search_data_points(Row, S) end, Rows),
    Results1 = lists:foldl(fun({error, _}=Error, _) -> Error;
                              (_, {error, _}=Error) -> Error;
                              ({ok, DataPoints}, Acc) -> lists:append(DataPoints,Acc) end,
                           [], Results0),
    case Results1 of
        {error, _} = Error -> Error;
        _ -> {ok, lists:reverse(Results1)}
    end.

select_data_points_query(RowKey, RowTime, #search{start_time=StartTime,
                                                  end_time=undefined,
                                                  order=Order, source=Source}) ->
    ReadsRowSize = mets:get_backend_env(mets_backend_sqlite, reads_row_size,
                                        ?DEFAULT_READS_ROW_SIZE),
    StartOffsetBin  = start_time_bin(StartTime, RowTime, Source),
    {?SELECT_DATA_FROM_START(Order), [RowKey, StartOffsetBin, ReadsRowSize]};
select_data_points_query(RowKey, RowTime, #search{start_time=StartTime, end_time=EndTime,
                                                  order=Order, source=Source}) ->
    ReadsRowSize = mets:get_backend_env(mets_backend_sqlite, reads_row_size,
                                        ?DEFAULT_READS_ROW_SIZE),
    StartOffsetBin  = start_time_bin(StartTime, RowTime, Source),
    EndOffsetBin    = end_time_bin(EndTime, RowTime, Source),
    {?SELECT_DATA_IN_RANGE(Order), [RowKey, StartOffsetBin, EndOffsetBin, ReadsRowSize]}.

do_search_data_points(Row, S) ->
    do_search_data_points(Row, S, []).

do_search_data_points({RowKey, RowProps}=Row, #search{}=S, Acc) ->
    ReadsRowSize = mets:get_backend_env(mets_backend_sqlite, reads_row_size,
                                        ?DEFAULT_READS_ROW_SIZE),
    RowTime = proplists:get_value(row_time, RowProps),
    Qry = select_data_points_query(RowKey, RowTime, S),
    QueryResult = select(Qry),
    case QueryResult of
        {ok, BinDataPoints} ->
            DataPoints = lists:map(to_data_point(RowProps), BinDataPoints),
            case length(DataPoints) == ReadsRowSize of
                true ->
                    continue_query_data_points(DataPoints, Row, S, Acc);
                false ->
                    {ok, lists:reverse(lists:append(DataPoints,Acc))}
            end;
        {error, not_found} ->
            {ok, lists:reverse(Acc)};
        {error, _} = Error ->
            Error
    end.

continue_query_data_points(DataPoints, Row, #search{order=asc}=S, Acc) ->
    LastDP = lists:last(DataPoints),
    LastTs = proplists:get_value(ts, LastDP),
    NewStartTime = LastTs+1,
    do_search_data_points(Row, S#search{start_time=NewStartTime},
                          lists:append(DataPoints,Acc));
continue_query_data_points(DataPoints, Row, #search{order=desc}=S, Acc) ->
    LastDP = lists:last(DataPoints),
    LastTs = proplists:get_value(ts, LastDP),
    NewEndTime = LastTs-1,
    do_search_data_points(Row, S#search{end_time=NewEndTime},
                          lists:append(DataPoints,Acc)).

start_time_bin(StartTime, RowTime, Source) ->
    MaxTime = lists:max([StartTime, RowTime]),
    {ok, Bin} = mets_blobs:encode_timestamp(MaxTime, Source),
    Bin.

end_time_bin(EndTime, RowTime, Source) ->
    RowWidth   = mets_utils:row_width(Source),
    RowWidthMs = mets_utils:ms(RowWidth),
    {ok, Bin} = case EndTime > (RowTime + RowWidthMs) of
                    true ->
                        mets_blobs:encode_offset(RowWidthMs+1);
                    false ->
                        mets_blobs:encode_timestamp(EndTime, Source)
                end,
    Bin.

to_data_point(RowProps) ->
    RowTime      = proplists:get_value(row_time, RowProps),
    Tags         = proplists:get_value(tags, RowProps),
    Aggregate    = proplists:get_value(aggregate, RowProps),
    AggregateMod = mets_aggregator:mod(Aggregate),
    fun([BinOffset, BinValue]) ->
        {ok, Ts} = mets_blobs:decode_timestamp(BinOffset, RowTime),
        {ok, Value} = mets_blobs:decode_aggregate_state(BinValue),
        [{tags, Tags}, {ts, Ts},{value, AggregateMod:value(Value)}]
    end.
