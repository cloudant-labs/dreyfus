% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(search_index_ddoc_update_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("dreyfus.hrl").

-define(DDOC, {[
    {<<"_id">>, <<"_design/searchddoc">>},
    {<<"indexes">>, {[
        {<<"other">>, {[
            {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"blah\\\", doc.blah);}}">>}
        ]}},
        {<<"default">>, {[
            {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"default\\\", doc.blah);}}">>}
        ]}}
    ]}}
]}).

-define(USER, "admin").
-define(PASS, "pass").
-define(AUTH, {basic_auth, {?USER, ?PASS}}).


start() ->
    Ctx = test_util:start_couch([chttpd, dreyfus]),
    ok = config:set("admins", ?USER, ?PASS, _Persist=false),
    Ctx.

setup() ->
    DbName = ?tempdb(),
    ok = create_db(DbName),

    ok = meck:new(mochiweb_socket, [passthrough]),
    fake_clouseau(),
    fake_dreyfus_index(),

    upload_ddoc(?b2l(DbName), ?DDOC),
    ?b2l(DbName).

teardown(DbName) ->
    (catch meck:unload(mochiweb_socket)),
    (catch meck:unload(clouseau_rpc)),
    (catch meck:unload(dreyfus_index)),
    delete_db(?l2b(DbName)),
    ok.

search_test_() ->
    {
        "Check search index closes after delete",
        {
            setup,
            fun start/0, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun should_stop_index_after_deleting_index/1,
                    fun should_stop_index_after_deleting_ddoc/1,
                    fun should_not_stop_index_after_adding_new_index/1,
                    fun should_not_stop_index_if_index_referred_from_other_ddoc/1
                ]
            }
        }
    }.

fake_clouseau() ->
    ok = meck:new([clouseau_rpc], [non_strict]),
    ok = meck:expect(clouseau_rpc, open_index, ['_', '_', '_'], {ok, self()}),
    ok = meck:expect(clouseau_rpc, await, ['_', '_'], ok),
    ok = meck:expect(clouseau_rpc, get_update_seq, ['_'], {ok, 10}),
    ok = meck:expect(clouseau_rpc, info, ['_'], {ok, [{disk_size, 10},
      {doc_count, 10},
      {doc_del_count, 0},
      {pending_seq, 11},
      {committed_seq, 10}]}),
    ok = meck:expect(clouseau_rpc, search, ['_', '_'], {ok, [
              {update_seq, 10},
              {total_hits, 0},
              {hits, []}
            ]}),
    ok = meck:expect(clouseau_rpc, update, ['_', '_', '_'], ok),
    ok = meck:expect(clouseau_rpc, commit, ['_', '_'], ok),
    ok = meck:expect(clouseau_rpc, search, ['_', '_', '_', '_', '_', '_'], {ok, [
              {update_seq, 10},
              {total_hits, 0},
              {hits, []}
            ]}).

fake_dreyfus_index() ->
    ok = meck:new([dreyfus_index], [passthrough]),
    ok = meck:expect(dreyfus_index, await, ['_', '_'], ok),
    ok = meck:expect(dreyfus_index, search, ['_', '_'], {ok, #top_docs{
               update_seq = 10,
               total_hits = 0,
               hits = []
              }}).

should_stop_index_after_deleting_index(DbName) ->
    {timeout, 60,
     ?_test(begin
         ok = meck:reset(clouseau_rpc),
         ok = meck:reset(dreyfus_index),
         ok = create_doc(DbName, <<"doc_id">>, {[]}),
         ReqUrl = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc/_search/other?q=*:*",
         {ok, Status, _Headers, _Body} =
              test_request:get(ReqUrl, [?AUTH]),
          ?assertEqual(200, Status),
          DDocId = "_design/searchddoc",
          IndexPids = ensure_index_is_opened(DbName, DDocId, "other"),

          % Upload the new DDoc which will delete the search index named "other". And verify that the index will be closed
          UpdatedDDocJsonObj = ddoc_delete_index_json_obj(DbName, ?l2b(DDocId)),
          upload_ddoc(DbName, UpdatedDDocJsonObj),
          ensure_index_is_closed(DbName, "_design/searchddoc", "other", IndexPids)
    end)}.

should_stop_index_after_deleting_ddoc(DbName) ->
    {timeout, 60,
     ?_test(begin
         ok = meck:reset(clouseau_rpc),
         ok = meck:reset(dreyfus_index),
         ok = create_doc(DbName, <<"doc_id">>, {[]}),
         ReqUrl = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc/_search/other?q=*:*",
         {ok, Status, _Headers, _Body} =
              test_request:get(ReqUrl, [?AUTH]),
          ?assertEqual(200, Status),
          DDocId = "_design/searchddoc",
          IndexPids = ensure_index_is_opened(DbName, DDocId, "other"),

          % Delete the design doc and verify that the index will be closed
          delete_ddoc(DbName, ?l2b(DDocId)),
          ensure_index_is_closed(DbName, "_design/searchddoc", "other", IndexPids)
    end)}.

should_not_stop_index_after_adding_new_index(DbName) ->
    {timeout, 60,
     ?_test(begin
         ok = meck:reset(clouseau_rpc),
         ok = meck:reset(dreyfus_index),
         ok = create_doc(DbName, <<"doc_id">>, {[]}),
         ReqUrl = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc/_search/other?q=*:*",
         {ok, Status, _Headers, _Body} =
              test_request:get(ReqUrl, [?AUTH]),
          ?assertEqual(200, Status),
          DDocId = "_design/searchddoc",
          ensure_index_is_opened(DbName, DDocId, "other"),

          % Upload the new DDoc which will delete the search index named "other". And verify that the index will be closed
          UpdatedDDocJsonObj = ddoc_add_index_json_obj(DbName, ?l2b(DDocId)),
          upload_ddoc(DbName, UpdatedDDocJsonObj),
          ensure_index_is_opened(DbName, DDocId, "other")
    end)}.

should_not_stop_index_if_index_referred_from_other_ddoc(DbName) ->
    {timeout, 60,
     ?_test(begin
         ok = meck:reset(clouseau_rpc),
         ok = meck:reset(dreyfus_index),
         ok = create_doc(DbName, <<"doc_id">>, {[]}),

         % Create another design doc with the same indices (with same sig)
         OtherDDocJsonObj = ddoc_second_design_doc_with_same_index_json_obj(DbName),
         upload_ddoc2(DbName, OtherDDocJsonObj),

         ReqUrl = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc/_search/other?q=*:*",
         {ok, Status, _Headers, _Body} =
              test_request:get(ReqUrl, [?AUTH]),
          ?assertEqual(200, Status),
          DDocId = "_design/searchddoc",
          ensure_index_is_opened(DbName, DDocId, "other"),

          DeleteIndexDDocJsonObj = ddoc_delete_index_json_obj(DbName, ?l2b(DDocId)),
          upload_ddoc(DbName, DeleteIndexDDocJsonObj),

          % As the index is referred from the other design doc, index should not be closed even if it's deleted from one design doc
          ensure_index_is_opened(DbName, "_design/searchddoc2", "other")
    end)}.

ddoc_delete_index_json_obj(DbName, DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<DDocId/binary>>, [ejson_body, ?ADMIN_CTX]),
    UpdatedDDocJsonObj =
        {[
            {<<"_id">>, <<"_design/searchddoc">>},
            {<<"_rev">>, ?b2l(couch_doc:rev_to_str(DDoc#doc.revs))},
            {<<"indexes">>, {[
                {<<"default">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"default\\\", doc.blah);}}">>}
                ]}}
            ]}}
        ]},
    UpdatedDDocJsonObj.

ddoc_add_index_json_obj(DbName, DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<DDocId/binary>>, [ejson_body, ?ADMIN_CTX]),
    UpdatedDDocJsonObj =
        {[
            {<<"_id">>, <<"_design/searchddoc">>},
            {<<"_rev">>, ?b2l(couch_doc:rev_to_str(DDoc#doc.revs))},
            {<<"indexes">>, {[
                {<<"other">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"blah\\\", doc.blah);}}">>}
                ]}},
                {<<"other1">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"newfield\\\", doc.blah);}}">>}
                ]}},
                {<<"default">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"default\\\", doc.blah);}}">>}
                ]}}
            ]}}
        ]},
    UpdatedDDocJsonObj.

ddoc_second_design_doc_with_same_index_json_obj(_DbName) ->
    UpdatedDDocJsonObj =
        {[
            {<<"_id">>, <<"_design/searchddoc2">>},
            {<<"indexes">>, {[
                {<<"other">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"blah\\\", doc.blah);}}">>}
                ]}},
                {<<"default">>, {[
                    {<<"index">>, <<"function(doc){if(doc.blah){index(\\\"default\\\", doc.blah);}}">>}
                ]}}
            ]}}
        ]},
    UpdatedDDocJsonObj.

ensure_index_is_opened(DbName, DDocId, IndexName) ->
    IndexWithName = get_index(DbName, ?l2b(DDocId), ?l2b(IndexName)),
    ?assertMatch(#index{sig=_Sig}, IndexWithName),
    [#index{sig=Sig}] = [IndexWithName],
    IndexPids = get_index_by_sig_from_ets(Sig),
    ?assert(length(IndexPids) > 0),
    AliveBefore = lists:filter(fun erlang:is_process_alive/1, IndexPids),
    ?assertEqual(length(IndexPids), length(AliveBefore)),
    IndexPids.

ensure_index_is_closed(DbName, DDocId, IndexName, IndexPids) ->
    NotAnIndex = get_index(DbName, ?l2b(DDocId), ?l2b(IndexName)),
    ?assertEqual(ok, NotAnIndex),
    (catch wait_for_indices_to_stop(?l2b(DbName))),
    AliveAfter = lists:filter(fun erlang:is_process_alive/1, IndexPids),
    ?assertEqual(0, length(AliveAfter)),
    ok.

get_index(DbName, DDocId, IndexName) ->
    case fabric:open_doc(DbName, <<DDocId/binary>>, [ejson_body, ?ADMIN_CTX]) of
        {ok, DDoc} ->
            case dreyfus_index:design_doc_to_index(DDoc, IndexName) of
                {ok, Index} ->
                    Index;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

get_index_by_sig_from_ets(Sig) ->
    Indexes = ets:match_object(
        dreyfus_by_db, {'_', {'_', Sig}}),
    lists:foldl(fun({DbName, {_DDocId, _Sig}}, Acc) ->
        case ets:lookup(dreyfus_by_sig, {DbName, Sig}) of
            [{_, Pid}] -> [Pid| Acc];
            _ -> Acc
        end
    end, [], Indexes).

wait_for_indices_to_stop(DbName) ->
    DbDir = config:get("couchdb", "database_dir", "."),
    WaitFun = fun() ->
        filelib:fold_files(DbDir, <<".*", DbName/binary, "\\.[0-9]+.*">>,
            true, fun(_F, _A) -> wait end, ok)
    end,
    ok = test_util:wait(WaitFun).

create_doc(DbName, Id, Body) ->
    JsonDoc = couch_util:json_apply_field({<<"_id">>, Id}, Body),
    Doc = couch_doc:from_json_obj(JsonDoc),
    {ok, _} = fabric:update_docs(DbName, [Doc], [?ADMIN_CTX]),
    ok.

create_db(DbName) ->
    {ok, Status, _, _} = test_request:put(db_url(DbName), [?AUTH], ""),
    assert_success(create_db, Status),
    ok.

delete_db(DbName) ->
    {ok, Status, _, _} = test_request:delete(db_url(DbName), [?AUTH]),
    assert_success(delete_db, Status),
    ok.

assert_success(create_db, Status) ->
    ?assert(lists:member(Status, [201, 202]));
assert_success(delete_db, Status) ->
    ?assert(lists:member(Status, [200, 202])).

host_url() ->
    "http://" ++ bind_address() ++ ":" ++ port().

bind_address() ->
    config:get(section(), "bind_address", "127.0.0.1").

section() -> "chttpd".

db_url(DbName) when is_binary(DbName) ->
    db_url(binary_to_list(DbName));
db_url(DbName) when is_list(DbName) ->
    host_url() ++ "/" ++ DbName.

port() ->
    integer_to_list(mochiweb_socket_server:get(chttpd, port)).

upload_ddoc(DbName, DDocJson) ->
    Url = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc",
    Body = couch_util:json_encode(DDocJson),
    {ok, 201, _Resp, _Body} = test_request:put(Url, [?AUTH], Body),
    ok.

upload_ddoc2(DbName, DDocJson) ->
    Url = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc2",
    Body = couch_util:json_encode(DDocJson),
    {ok, 201, _Resp, _Body} = test_request:put(Url, [?AUTH], Body),
    ok.

delete_ddoc(DbName, DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<DDocId/binary>>, [ejson_body, ?ADMIN_CTX]),
    Url = host_url() ++ "/" ++ DbName ++ "/_design/searchddoc" ++ "?rev=" ++ ?b2l(couch_doc:rev_to_str(DDoc#doc.revs)),
    {ok, 200, _, _} = test_request:delete(Url, [?AUTH]),
    ok.
