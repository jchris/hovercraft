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
    delete_doc/2,
    start_attachment/3,
    next_attachment_bytes/1,
    query_view/4 % coming soon
]).

-include("../../couchdb/include/couch_db.hrl").


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
    delete_db(DbName,  [{user_ctx, #user_ctx{}}]).
    
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
%% Function: open_doc(DbName, DocId) -> {ok, {DocId, RevString}} | {error,Error}
%% Description: Gets the eJSON form of the db_info
%%--------------------------------------------------------------------
save_doc(DbName, {DocProps}) ->
    {ok, Db} = open_db(DbName),
    Doc = case proplists:get_value(<<"_id">>, DocProps) of
        undefined ->
            DocId = couch_util:new_uuid(),
            {[{<<"_id">>, DocId}|DocProps]};
        DocId ->
            {DocProps}
    end,
    CouchDoc = couch_doc:from_json_obj(Doc),
    {ok, Rev} = couch_db:update_doc(Db, CouchDoc, []),
    {ok, {[{<<"_id">>, DocId}, {<<"_rev">>, couch_doc:rev_to_str(Rev)}]}}.

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
    
query_view(Pid, DName, VName, QueryArgs) ->
    ok.



%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

open_db(DbName) ->
    couch_db:open(DbName, [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}]).


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
    ?LOG_INFO("Starting tests for ~p", [DbName]),
    should_create_db(DbName),
    should_link_to_db_server(DbName),
    should_get_db_info(DbName),
    should_save_and_open_doc(DbName),
    should_stream_attachment(DbName),
    should_query_view(DbName),
    should_error_on_missing_doc(DbName),
    ok.
    
should_create_db(DbName) ->
    % ?LOG("should_create_db",""),
    hovercraft:delete_db(DbName),
    {ok, created} = hovercraft:create_db(DbName),
    % ?LOG("Db Created", DbName),
    {ok, DbInfo} = hovercraft:db_info(DbName).
    
should_link_to_db_server(DbName) ->
    {ok, DbInfo} = hovercraft:db_info(DbName),
    % ?LOG("DbInfo", DbInfo),
    DbName = proplists:get_value(db_name, DbInfo).

should_get_db_info(DbName) ->
    {ok, DbInfo} = hovercraft:db_info(DbName),
    DbName = proplists:get_value(db_name, DbInfo),
    0 = proplists:get_value(doc_count, DbInfo),
    0 = proplists:get_value(update_seq, DbInfo).   

should_save_and_open_doc(DbName) ->
    Doc = {[{<<"foo">>, <<"bar">>}]},
    {ok, {Resp}} = hovercraft:save_doc(DbName, Doc),
    DocId = proplists:get_value(<<"_id">>, Resp),
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
    DocId = proplists:get_value(<<"_id">>, Resp),
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
    

should_query_view(DbName) ->
    ok.
