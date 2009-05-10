%%%-------------------------------------------------------------------
%%% File    : hovercraft.erl
%%% Author  : J Chris Anderson <jchris@couch.io>
%%% Description : Erlang CouchDB access.
%%%
%%% Created :  4 Apr 2009 by J Chris Anderson <jchris@couch.io>
%%%-------------------------------------------------------------------
-module(hovercraft).

%% Test API
-export([test/0]).

%% Database API
-export([
    create_db/1,
    delete_db/1,
    db_info/1,
    open_doc/2, 
    save_doc/2,
    save_bulk/2,
    delete_doc/2,
    start_attachment/3,
    next_attachment_bytes/1,
    query_view/5 % coming soon
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
save_doc(DbName, Doc) ->
    {ok, Db} = open_db(DbName),
    CouchDoc = ejson_to_couch_doc(Doc),
    {ok, Rev} = couch_db:update_doc(Db, CouchDoc, []),
    {ok, {[{id, CouchDoc#doc.id}, {rev, couch_doc:rev_to_str(Rev)}]}}.

%%--------------------------------------------------------------------
%% Function: save_bulk(DbName, Docs) -> {ok, EJsonInfo} | {error,Error}
%% Description: Saves the docs the database, returns the ids and revs
%%--------------------------------------------------------------------
save_bulk(DbName, Docs) ->
    {ok, Db} = open_db(DbName),
    CouchDocs = [ejson_to_couch_doc(EJsonDoc) || EJsonDoc <- Docs],
    {ok, Results} = couch_db:update_docs(Db, CouchDocs),
    {ok, lists:zipwith(fun couch_httpd_db:update_doc_result_to_json/2,
        CouchDocs, Results)}.
    

%%--------------------------------------------------------------------
%% Function: delete_doc(DbName, DocId) -> {ok, {DocId, RevString}} | {error,Error}
%% Description: Deletes the doc
%%--------------------------------------------------------------------
delete_doc(DbName, {DocProps}) ->
    save_doc(DbName, {[{<<"deleted">>, true}|DocProps]}).


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
    
query_view(DbName, DesignName, ViewName, ViewFoldFun, #view_query_args{
            limit = Limit,
            skip = SkipCount,
            stale = Stale,
            direction = Dir,
            start_key = StartKey,
            start_docid = StartDocId,
            end_key = EndKey
        }=QueryArgs) ->
    {ok, Db} = open_db(DbName),
    % get view reference
    {ok, View, Group} = couch_view:get_map_view(Db, 
        <<"_design/", DesignName/binary>>, ViewName, Stale),
    {ok, RowCount} = couch_view:get_row_count(View),
    Start = {StartKey, StartDocId},
    FoldlFun = couchdb_httpd_view:make_view_fold_fun(nil, QueryArgs, <<"">>, Db, RowCount, 
        #view_fold_helper_funs{
            reduce_count = fun couch_view:reduce_to_count/1,
            start_response = fun start_map_view_fold_fun/4,
            send_row = fun map_row_fold_fun/4
        }),
    FoldAccInit = {Limit, SkipCount, undefined, []},
    FoldResult = couch_view:fold(View, Start, Dir, FoldlFun, FoldAccInit),
    {ok, FoldResult}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

open_db(DbName) ->
    couch_db:open(DbName, [?ADMIN_USER_CTX]).

ejson_to_couch_doc({DocProps}) ->
    Doc = case proplists:get_value(<<"_id">>, DocProps) of
        undefined ->
            DocId = couch_util:new_uuid(),
            {[{<<"_id">>, DocId}|DocProps]};
        DocId ->
            {DocProps}
    end,
    couch_doc:from_json_obj(Doc).

start_map_view_fold_fun(Req2, _Etag, TotalViewCount, Offset) ->
    ok.

map_row_fold_fun(Resp, Db2, {{Key, DocId}, Value}, {RowFront, _IncludeDocs}) ->
    ok.

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
    % should_query_view(DbName),
    should_error_on_missing_doc(DbName),
    should_save_bulk_docs(DbName),
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
    
% TODO: after CouchDB refactor
should_query_view(DbName) ->
    % make ddoc
    {ok, {Resp}} = hovercraft:save_doc(DbName, make_test_ddoc(<<"view-test">>)),
    % make docs
    ok = make_test_docs(DbName, 20),
    % query view
    FoldFun = fun
        (pattern) when guard ->
            body
    end,
    Rows = hovercraft:query_view(DbName, <<"view-test">>, <<"basic">>, FoldFun),
    ok.

make_test_ddoc(DesignName) ->
    {[
        {<<"_id">>, <<"_design/", DesignName/binary>>},
        {<<"views">>, {[{<<"basic">>, 
            {[
                {<<"map">>, <<"function(doc) { emit(doc._rev, 1) }">>}
            ]}
        }]}}
    ]}.

make_test_docs(DbName, Count) ->
    {ok, _} = lists:foldl(fun(S) -> 
            {ok, _} = hovercraft:save_doc(DbName, {[{<<"foo">>, <<"bar">>}]})
        end, lists:seq(0, Count)),
    ok.

should_save_bulk_docs(DbName) ->
    Docs = [{[{<<"foo">>, <<"bar">>}]} || Seq <- lists:seq(1, 100)],
    {ok, RevInfos} = hovercraft:save_bulk(DbName, Docs),
    lists:foldl(fun(Row, _) ->
            DocId = proplists:get_value(id, Row),
            {ok, _Doc} = hovercraft:open_doc(DbName, DocId)
        end, RevInfos, []).

    
% create as many docs as possible in 30 seconds
lightning(NumDocs) ->
    BulkSize = 1000,
    Doc = {[{<<"foo">>, <<"bar">>}]}.


insert_doc_group(Db, Doc, GroupSize, IdInt) ->
    % {DocsList, IdInt1} = make_docs_list(Doc, GroupSize, [], IdInt),
    % {ok, _Revs} = couch_db:update_docs(Db, DocsList),
    IdInt.
