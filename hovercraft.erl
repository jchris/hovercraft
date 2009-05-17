%%%-------------------------------------------------------------------
%%% File    : hovercraft.erl
%%% Author  : J Chris Anderson <jchris@couch.io>
%%% Description : Erlang CouchDB access.
%%%
%%% Created : 4 Apr 2009 by J Chris Anderson <jchris@couch.io>
%%% License : Apache 2.0
%%%-------------------------------------------------------------------
-module(hovercraft).
-vsn("0.1.1").

%% see README.mk for usage information

%% Test API
-export([test/0, lightning/0, lightning/1]).

%% Database API
-export([
    create_db/1,
    delete_db/1,
    open_db/1,
    db_info/1,
    open_doc/2, 
    save_doc/2,
    save_bulk/2,
    delete_doc/2,
    start_attachment/3,
    next_attachment_bytes/1,
    query_view/3,
    query_view/4,
    query_view/5
]).

% Hovercraft is designed to run inside the same beam process as CouchDB.
% It calls into CouchDB's database access functions, so it needs access
% to the disk. To hook it up, make sure hovercraft is in the Erlang load 
% path and ensure that the following line points to CouchDB's hrl file.
-include("src/couchdb/couch_db.hrl").

-define(ADMIN_USER_CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>]}}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: create_db(DbName) -> {ok,created} | {error,Error}
%% DbName is always a binary: <<"mydb">>
%% Description: Creates the database.
%%--------------------------------------------------------------------

create_db(DbName) ->
    create_db(DbName, []).

create_db(DbName, Options) ->
    case couch_server:create(DbName, Options) of
    {ok, Db} ->
        couch_db:close(Db),
        {ok, created};
    Error ->
        {error, Error}
    end.

%%--------------------------------------------------------------------
%% Function: delete_db(DbName) -> {ok,deleted} | {error,Error}
%% Description: Deletes the database.
%%--------------------------------------------------------------------
delete_db(DbName) ->
    delete_db(DbName,  [?ADMIN_USER_CTX]).
    
delete_db(DbName, Options) ->
    case couch_server:delete(DbName, Options) of
    ok ->
        {ok, deleted};
    Error ->
        {error, Error}
    end.

%%--------------------------------------------------------------------
%% Function: open_db(DbName) -> {ok,Db} | {error,Error}
%% Description: Opens the database.
%% Only use this if you are doing bulk operations and know what you are doing.
%%--------------------------------------------------------------------
open_db(DbName) ->
    couch_db:open(DbName, [?ADMIN_USER_CTX]).

%%--------------------------------------------------------------------
%% Function: db_info(DbName) -> {ok,created} | {error,Error}
%% Description: Gets the db_info as a proplist
%%--------------------------------------------------------------------
db_info(DbName) ->
    {ok, Db} = open_db(DbName),
    couch_db:get_db_info(Db).

%%--------------------------------------------------------------------
%% Function: open_doc(DbName, DocId) -> {ok,created} | {error,Error}
%% Description: Gets the eJSON form of the db_info
%%--------------------------------------------------------------------
open_doc(DbName, DocId) ->
    {ok, Db} = open_db(DbName),
    CouchDoc = couch_httpd_db:couch_doc_open(Db, DocId, nil, []),
    Doc = couch_doc:to_json_obj(CouchDoc, []),
    {ok, Doc}.
        
%%--------------------------------------------------------------------
%% Function: save_doc(DbName, Doc) -> {ok, EJsonInfo} | {error,Error}
%% Description: Saves the doc the database, returns the id and rev
%%--------------------------------------------------------------------
save_doc(#db{}=Db, Doc) ->
    CouchDoc = ejson_to_couch_doc(Doc),
    {ok, Rev} = couch_db:update_doc(Db, CouchDoc, []),
    {ok, {[{id, CouchDoc#doc.id}, {rev, couch_doc:rev_to_str(Rev)}]}};
save_doc(DbName, Docs) ->
    {ok, Db} = open_db(DbName),
    save_doc(Db, Docs).

%%--------------------------------------------------------------------
%% Function: save_bulk(DbName, Docs) -> {ok, EJsonInfo} | {error,Error}
%% Description: Saves the docs the database, returns the ids and revs
%%--------------------------------------------------------------------
save_bulk(#db{}=Db, Docs) ->
    CouchDocs = [ejson_to_couch_doc(EJsonDoc) || EJsonDoc <- Docs],
    {ok, Results} = couch_db:update_docs(Db, CouchDocs),
    {ok, lists:zipwith(fun couch_httpd_db:update_doc_result_to_json/2,
        CouchDocs, Results)};
save_bulk(DbName, Docs) ->
    {ok, Db} = open_db(DbName),
    save_bulk(Db, Docs).
    

%%--------------------------------------------------------------------
%% Function: delete_doc(DbName, DocId) -> {ok, {DocId, RevString}} | {error,Error}
%% Description: Deletes the doc
%%--------------------------------------------------------------------
delete_doc(DbName, {DocProps}) ->
    save_doc(DbName, {[{<<"_deleted">>, true}|DocProps]}).


%%--------------------------------------------------------------------
%% Function: start_attachment(DbName, DocId, AName) -> {ok, Pid} | {error,Error}
%% Description: Starts a process to use for fetching attachment bytes
%%--------------------------------------------------------------------
start_attachment(DbName, DocId, AName) -> 
    Pid = spawn_link(fun() -> 
            attachment_streamer(DbName, DocId, AName)
        end),
    {ok, Pid}.

%%--------------------------------------------------------------------
%% Function: start_attachment(DbName, DocId, AName) -> {ok, Pid} | {error,Error}
%% Description: Starts a process to use for fetching attachment bytes
%%--------------------------------------------------------------------
next_attachment_bytes(Pid) ->
    Pid ! {next_attachment_bytes, self()},
    receive
        {attachment_done, Pid} ->
            {ok, done};
        {attachment_bytes, Pid, {Bin, Length}} ->
           {Length, _Reason} = {size(Bin), "assert Bin length match"},
           {ok, Bin}
    end.

query_view(DbName, DesignName, ViewName) ->
    query_view(DbName, DesignName, ViewName, #view_query_args{}).
   
query_view(DbName, DesignName, ViewName, #view_query_args{}=QueryArgs) ->
    % provide a default row collector fun
    % don't use this on big data it will balloon in memory
    RowCollectorFun = fun(Row, Acc) ->
        {ok, [Row | Acc]}
    end,
    query_view(DbName, DesignName, ViewName, RowCollectorFun, QueryArgs);

query_view(DbName, DesignName, ViewName, ViewFoldFun) ->
    query_view(DbName, DesignName, ViewName, ViewFoldFun, #view_query_args{}).

    
query_view(DbName, DesignName, ViewName, ViewFoldFun, #view_query_args{
            limit = Limit,
            skip = SkipCount,
            stale = Stale,
            direction = Dir,
            group_level = GroupLevel,
            start_key = StartKey,
            start_docid = StartDocId,
            end_key = EndKey,
            end_docid = EndDocId
        }=QueryArgs) ->
    {ok, Db} = open_db(DbName),
    % get view reference
    DesignId = <<"_design/", DesignName/binary>>,
    case couch_view:get_map_view(Db, DesignId, ViewName, Stale) of
        {ok, View, Group} ->
            {ok, RowCount} = couch_view:get_row_count(View),
            Start = {StartKey, StartDocId},
            FoldlFun = couch_httpd_view:make_view_fold_fun(nil, 
                QueryArgs, <<"">>, Db, RowCount, 
                #view_fold_helper_funs{
                    reduce_count = fun couch_view:reduce_to_count/1,
                    start_response = fun start_map_view_fold_fun/5,
                    send_row = make_map_row_fold_fun(ViewFoldFun)
                }),
            FoldAccInit = {Limit, SkipCount, undefined, []},
            {ok, {_, _, _, {Offset, ViewFoldAcc}}} = 
                couch_view:fold(View, Start, Dir, FoldlFun, FoldAccInit),
            {ok, {RowCount, Offset, ViewFoldAcc}};
        {not_found, Reason} ->
            case couch_view:get_reduce_view(Db, DesignId, ViewName, Stale) of
                {ok, View, Group} ->
                    {ok, GroupRowsFun, RespFun} = 
                        couch_httpd_view:make_reduce_fold_funs(nil, 
                            GroupLevel, QueryArgs, <<"">>, 
                            #reduce_fold_helper_funs{
                                start_response = fun start_reduce_view_fold_fun/3,
                                send_row = make_reduce_row_fold_fun(ViewFoldFun)
                            }),
                    FoldAccInit = {Limit, SkipCount, undefined, []},
                    {ok, {_, _, _, AccResult}} = couch_view:fold_reduce(View, Dir, {StartKey, StartDocId}, 
                        {EndKey, EndDocId}, GroupRowsFun, RespFun, FoldAccInit),
                    {ok, AccResult};
                _ ->
                    throw({not_found, Reason})
            end
    end.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

ejson_to_couch_doc({DocProps}) ->
    Doc = case proplists:get_value(<<"_id">>, DocProps) of
        undefined ->
            DocId = couch_util:new_uuid(),
            {[{<<"_id">>, DocId}|DocProps]};
        DocId ->
            {DocProps}
    end,
    couch_doc:from_json_obj(Doc).

start_map_view_fold_fun(_Req, _Etag, _RowCount, Offset, Acc) ->
    {ok, nil, {Offset, []}}.

make_map_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, _Db, {{Key, DocId}, Value}, _IncludeDocs, {Offset, Acc}) ->
        {Go, NewAcc} = ViewFoldFun({{Key, DocId}, Value}, Acc),
        {Go, {Offset, NewAcc}}
    end.

start_reduce_view_fold_fun(Req, Etag, _Acc0) ->
    {ok, nil, []}.

make_reduce_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, {Key, Value}, Acc) ->
        {Go, NewAcc} = ViewFoldFun({Key, Value}, Acc),
        {Go, NewAcc}
    end.


attachment_streamer(DbName, DocId, AName) ->
    {ok, Db} = open_db(DbName),
    case couch_db:open_doc(Db, DocId, []) of
    {ok, #doc{attachments=Attachments}=Doc} ->
        case proplists:get_value(AName, Attachments) of
        undefined ->
            throw({not_found, "Document is missing attachment"});
        {_Type, BinPointer} ->
            Me = self(),
            couch_doc:bin_foldl(BinPointer,
                fun(BinSegment, []) ->
                    receive
                        {next_attachment_bytes, From} ->
                            From ! {attachment_bytes, Me, {BinSegment, size(BinSegment)}}
                    end,
                    {ok, []}
                end,
                []
            ),            
            receive
                {next_attachment_bytes, From} ->
                    From ! {attachment_done, Me}
            end
        end;
    _Else ->
        throw({not_found, "Document not found"})
    end.

    
%%--------------------------------------------------------------------
%%% Tests
%%% Rock!
%%--------------------------------------------------------------------

test() ->
    test(<<"hovercraft-test">>).

test(DbName) ->
    ?LOG_INFO("Starting tests in ~p", [DbName]),
    should_create_db(DbName),
    should_link_to_db_server(DbName),
    should_get_db_info(DbName),
    should_save_and_open_doc(DbName),
    should_stream_attachment(DbName),
    should_query_views(DbName),
    should_error_on_missing_doc(DbName),
    should_save_bulk_docs(DbName),
    should_save_bulk_and_open_with_db(DbName),
    ok.
    
should_create_db(DbName) ->
    hovercraft:delete_db(DbName),
    {ok, created} = hovercraft:create_db(DbName),
    {ok, DbInfo} = hovercraft:db_info(DbName).
    
should_link_to_db_server(DbName) ->
    {ok, DbInfo} = hovercraft:db_info(DbName),
    DbName = proplists:get_value(db_name, DbInfo).

should_get_db_info(DbName) ->
    {ok, DbInfo} = hovercraft:db_info(DbName),
    DbName = proplists:get_value(db_name, DbInfo),
    0 = proplists:get_value(doc_count, DbInfo),
    0 = proplists:get_value(update_seq, DbInfo).   

should_save_and_open_doc(DbName) ->
    Doc = {[{<<"foo">>, <<"bar">>}]},
    {ok, {Resp}} = hovercraft:save_doc(DbName, Doc),
    DocId = proplists:get_value(id, Resp),
    {ok, {Props}} = hovercraft:open_doc(DbName, DocId),
    <<"bar">> = proplists:get_value(<<"foo">>, Props).

should_error_on_missing_doc(DbName) ->
    try hovercraft:open_doc(DbName, <<"not a doc">>)
    catch
        throw:{not_found,missing} ->
             ok
    end.

should_stream_attachment(DbName) ->
    % setup
    AName = <<"test.txt">>,
    ABytes = list_to_binary(string:chars($a, 10*1024, "")),
    Doc = {[{<<"foo">>, <<"bar">>},
        {<<"_attachments">>, {[{AName, {[
            {<<"content_type">>, <<"text/plain">>},
            {<<"data">>, couch_util:encodeBase64(ABytes)}
        ]}}]}}
    ]},
    {ok, {Resp}} = hovercraft:save_doc(DbName, Doc),
    DocId = proplists:get_value(id, Resp),
    % stream attachment
    {ok, Pid} = hovercraft:start_attachment(DbName, DocId, AName),
    {ok, Attachment} = get_full_attachment(Pid, []),
    ABytes = Attachment,
    ok.

get_full_attachment(Pid, Acc) ->
    Val = hovercraft:next_attachment_bytes(Pid),
    case Val of
        {ok, done} ->
            {ok, list_to_binary(lists:reverse(Acc))};
        {ok, Bin} ->
            get_full_attachment(Pid, [Bin|Acc])
    end.
    
should_query_views(DbName) ->
    % make ddoc
    DDocName = <<"view-test">>,
    {ok, {_Resp}} = hovercraft:save_doc(DbName, make_test_ddoc(DDocName)),
    % make docs
    {ok, _RevInfos} = make_test_docs(DbName, {[{<<"hovercraft">>, <<"views rule">>}]}, 20),
    should_query_map_view(DbName, DDocName),
    should_query_reduce_view(DbName, DDocName).
    
should_query_map_view(DbName, DDocName) ->
    % use the default query arguments and row collector function
    {ok, {RowCount, Offset, Rows}} = 
        hovercraft:query_view(DbName, DDocName, <<"basic">>),
    % assert rows is the right length
    20 = length(Rows),
    RowCount = length(Rows),
    0 = Offset,
    % assert we got every row
    lists:foldl(fun({{RKey, RDocId}, RValue}, _) -> 
            1 = RValue,
            {ok, {DocProps}} = hovercraft:open_doc(RDocId),
            RKey = proplists:get_value(<<"_rev">>, DocProps)
        end, Rows, []).

should_query_reduce_view(DbName, DDocName) ->
    {ok, [Result]} = 
        hovercraft:query_view(DbName, DDocName, <<"reduce-sum">>),
    {null, 20} = Result,
    {ok, Results} = 
        hovercraft:query_view(DbName, DDocName, <<"reduce-sum">>, #view_query_args{
            group_level = exact
        }),
    20 = length(Results).


make_test_ddoc(DesignName) ->
    {[
        {<<"_id">>, <<"_design/", DesignName/binary>>},
        {<<"views">>, {[
            {<<"basic">>, 
                {[
                    {<<"map">>, 
<<"function(doc) { if(doc.hovercraft == 'views rule') emit(doc._rev, 1) }">>}
                ]}
            },{<<"reduce-sum">>,
                {[
                    {<<"map">>,
<<"function(doc) { if(doc.hovercraft == 'views rule') emit(doc._rev, 1) }">>},
                    {<<"reduce">>,
                    <<"function(ks,vs,co){ return sum(vs)}">>}
                ]}
            }
        ]}}
    ]}.

make_test_docs(DbName, Doc, Count) ->
    Docs = [Doc || _Seq <- lists:seq(1, Count)],
    hovercraft:save_bulk(DbName, Docs).

should_save_bulk_docs(DbName) ->
    Docs = [{[{<<"foo">>, <<"bar">>}]} || _Seq <- lists:seq(1, 100)],
    {ok, RevInfos} = hovercraft:save_bulk(DbName, Docs),
    lists:foldl(fun(Row, _) ->
            DocId = proplists:get_value(id, Row),
            {ok, _Doc} = hovercraft:open_doc(DbName, DocId)
        end, RevInfos, []).

should_save_bulk_and_open_with_db(DbName) ->
    {ok, Db} = hovercraft:open_db(DbName),
    Docs = [{[{<<"foo">>, <<"bar">>}]} || _Seq <- lists:seq(1, 100)],
    {ok, RevInfos} = hovercraft:save_bulk(Db, Docs),
    lists:foldl(fun(Row, _) ->
            DocId = proplists:get_value(id, Row),
            {ok, _Doc} = hovercraft:open_doc(Db, DocId)
        end, RevInfos, []).

% and now for the performance testing section
lightning() ->
    lightning(1000).

lightning(BulkSize) ->
    lightning(BulkSize, 100000, <<"hovercraft-lightning">>).

lightning(BulkSize, NumDocs, DbName) ->
    hovercraft:delete_db(DbName),
    {ok, created} = hovercraft:create_db(DbName),
    {ok, Db} = hovercraft:open_db(DbName),
    Doc = {[{<<"foo">>, <<"bar">>}]},
    StartTime = now(),    
    insert_in_batches(Db, NumDocs, BulkSize, Doc, 0),
    Duration = timer:now_diff(now(), StartTime) / 1000000,
    io:format("Inserted ~p docs in ~p seconds with batch size of ~p. (~p docs/sec)~n",
        [NumDocs, Duration, BulkSize, NumDocs / Duration]).

insert_in_batches(Db, NumDocs, BulkSize, Doc, StartId) when NumDocs > 0 ->
    LastId = insert_batch(Db, BulkSize, Doc, StartId),
    insert_in_batches(Db, NumDocs - BulkSize, BulkSize, Doc, LastId+1);
insert_in_batches(_, _, _, _, _) ->
    ok.

insert_batch(Db, BulkSize, Doc, StartId) ->
    {ok, Docs, LastId} = make_docs_list(Doc, BulkSize, [], StartId),
    {ok, _RevInfos} = hovercraft:save_bulk(Db, Docs),
    LastId.

make_docs_list({_DocProps}, 0, Docs, Id) ->
    {ok, Docs, Id};
make_docs_list({DocProps}, Size, Docs, Id) ->
    ThisDoc = {[{<<"_id">>, make_id_str(Id)}|DocProps]},
    make_docs_list({DocProps}, Size - 1, [ThisDoc|Docs], Id + 1).

make_id_str(Num) ->
    ?l2b(lists:flatten(io_lib:format("~10..0B", [Num]))).
