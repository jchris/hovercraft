%%%-------------------------------------------------------------------
%%% File    : hovercraft.erl
%%% Author  : J Chris Anderson <jchris@couch.io>
%%% Description : Erlang CouchDB access.
%%%
%%% Created : 4 Apr 2009 by J Chris Anderson <jchris@couch.io>
%%% License : Apache 2.0
%%%-------------------------------------------------------------------
-module(hovercraft_test).
-vsn("0.1.1").

%% see README.md for usage information

-export([all/0, all/1, lightning/0, lightning/1]).

-include("src/couchdb/couch_db.hrl").

-define(ADMIN_USER_CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>]}}).


%%--------------------------------------------------------------------
%%% Tests : Public Test API
%%--------------------------------------------------------------------

all() ->
    all(<<"hovercraft-test">>).

all(DbName) ->
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
    chain(),
    ok.

chain() ->
    DbName = <<"chain-test">>,
    TargetName = <<"chain-results-test">>,
    should_create_db(DbName),
    should_create_db(TargetName),
    % make ddoc
    DDocName = <<"view-test">>,
    {ok, {_Resp}} = hovercraft:save_doc(DbName, make_test_ddoc(DDocName)),
    % make docs
    {ok, _RevInfos} = make_test_docs(DbName, {[{<<"lorem">>, <<"Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.">>}]}, 200),
    do_chain(DbName, <<"view-test">>, <<"letter-cloud">>, TargetName).

%% Performance Tests
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


%%--------------------------------------------------------------------
%%% Tests : Test Cases
%%--------------------------------------------------------------------

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
    % assert correct lengths
    20 = RowCount,
    20 = Offset,
    % assert we got every row
    lists:foldl(fun({{RKey, RDocId}, RValue}, _) ->
                    1 = RValue,
                    {ok, {DocProps}} = hovercraft:open_doc(RDocId),
                    RKey = proplists:get_value(<<"_rev">>, DocProps)
                end,
                Rows,
                []).

should_query_reduce_view(DbName, DDocName) ->
    {ok, [Result]} =
        hovercraft:query_view(DbName, DDocName, <<"reduce-sum">>),
    {null, 20} = Result,
    {ok, Results} =
        hovercraft:query_view(DbName, DDocName, <<"reduce-sum">>, #view_query_args{
            group_level = exact
        }),
    [{_,20}] = Results.


%%--------------------------------------------------------------------
%%% Helper Functions
%%--------------------------------------------------------------------

get_full_attachment(Pid, Acc) ->
    Val = hovercraft:next_attachment_bytes(Pid),
    case Val of
        {ok, done} ->
            {ok, list_to_binary(lists:reverse(Acc))};
        {ok, Bin} ->
            get_full_attachment(Pid, [Bin|Acc])
    end.

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
            },{<<"letter-cloud">>,
                {[
                    {<<"map">>,
                    <<"function(doc){if (doc.lorem) {
                        for (var i=0; i < doc.lorem.length; i++) {
                          var c = doc.lorem[i];
                          emit(c,1)
                        };
                    }}">>},
                    {<<"reduce">>,
                        <<"_count">>}
                ]}
            }
        ]}}
    ]}.

make_test_docs(DbName, Doc, Count) ->
    Docs = [Doc || _Seq <- lists:seq(1, Count)],
    hovercraft:save_bulk(DbName, Docs).

do_chain(SourceDbName, SourceDDocName, SourceView, TargetDbName) ->
    {ok, Db} = hovercraft:open_db(TargetDbName),
    CopyToDbFun = fun
        (Row, Acc) when length(Acc) > 50 ->
            hovercraft:save_bulk(Db, [row_to_doc(Row)|Acc]),
            {ok, []};
        (Row, Acc) ->
            {ok, [row_to_doc(Row)|Acc]}
    end,
    {ok, Acc1} = hovercraft:query_view(SourceDbName, SourceDDocName, SourceView, CopyToDbFun, #view_query_args{
        group_level = exact
    }),
    hovercraft:save_bulk(Db, Acc1),
    ok.

row_to_doc({Key, Value}) ->
    {[
    {<<"key">>, Key},
    {<<"value">>,Value}
    ]}.

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

