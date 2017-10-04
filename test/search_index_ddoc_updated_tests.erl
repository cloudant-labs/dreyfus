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

-module(search_index_ddoc_updated_tests).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").

-define(DDOCID1, <<"_design/ddoc1">>).
-define(DDOCID2, <<"_design/ddoc2">>).
-define(DDOCID3, <<"_design/ddoc3">>).

-define(IDXNAME, <<"index1">>).
-define(IDXNAME2, <<"index2">>).

-define(DDOC1, couch_doc:from_json_obj({[
    {<<"_id">>,?DDOCID1},
    {<<"indexes">>, {[
        {?IDXNAME, {[
            {<<"index">>,
                <<"function(doc){if(doc.f){index(\\\"f\\\", doc.f);}}">>}
        ]}}
    ]}}
]})).

-define(DDOC2, couch_doc:from_json_obj({[
    {<<"_id">>, ?DDOCID2},
    {<<"indexes">>, {[
        {?IDXNAME, {[
            {<<"index">>,
                <<"function(doc){if(doc.f){index(\\\"f\\\", doc.f);}}">>}
        ]}}
    ]}}
]})).

-define(DDOC3, couch_doc:from_json_obj({[
    {<<"_id">>, ?DDOCID3},
    {<<"indexes">>, {[
        {?IDXNAME, {[
            {<<"index">>,
                <<"function(doc){if(doc.f1){index(\\\"f1\\\", doc.f1);}}">>}
        ]}},
        {?IDXNAME2, {[
            {<<"index">>,
                <<"function(doc){if(doc.f2){index(\\\"f2\\\", doc.f2);}}">>}
        ]}}
    ]}}
]})).


-define(DDOC31IND,
   {[
        {?IDXNAME, {[
            {<<"index">>,
                <<"function(doc){if(doc.f11){index(\\\"f11\\\", doc.f11);}}">>}
        ]}},
        {?IDXNAME2, {[
            {<<"index">>,
                <<"function(doc){if(doc.f2){index(\\\"f2\\\", doc.f2);}}">>}
        ]}}
    ]}
).



setup() ->
    Name = ?tempdb(),
    couch_server:delete(Name, [?ADMIN_CTX]),
    {ok, Db} = couch_db:create(Name, [?ADMIN_CTX]),
    Db.

teardown(Db) ->
    couch_db:close(Db),
    couch_server:delete(Db#db.name, [?ADMIN_CTX]),
    ok.


ddoc_update_test_() ->
    {
        "Check ddoc update actions",
        {
            setup,
            fun() ->
                Ctx = test_util:start_couch([dreyfus]),
                fake_rexi(),
                fake_clouseau(),
                fake_dreyfus_index(),
                Ctx
            end,
            fun(Ctx) ->
                (catch meck:unload(rexi)),
                (catch meck:unload(clouseau_rpc)),
                (catch meck:unload(dreyfus_index)),
                test_util:stop_couch(Ctx)
            end,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun should_stop_indexes_on_delete_single_ddoc/1,
                    fun should_not_stop_indexes_on_delete_multiple_ddoc/1,
                    fun should_stop_indexes_on_update/1
                ]
            }
        }
    }.


should_stop_indexes_on_delete_single_ddoc(Db0) ->
    ?_test(begin
        {ok, _} = couch_db:update_docs(Db0, [?DDOC1], []),
        {ok, Db} = couch_db:reopen(Db0),
        {ok, DDoc1} = couch_db:open_doc(
            Db, ?DDOCID1, [ejson_body, ?ADMIN_CTX]),

        dreyfus_rpc:call(
            search, Db#db.name, DDoc1, ?IDXNAME, #index_query_args{}),
        IndsBefore = get_indexes_by_ddoc(Db#db.name, ?DDOCID1, 1),
        ?assertEqual(1, length(IndsBefore)),
        AliveBefore = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(1, length(AliveBefore)),

        % delete DDoc1
        DDocJson11 = couch_doc:from_json_obj({[
           {<<"_id">>, ?DDOCID1},
           {<<"_deleted">>, true},
           {<<"_rev">>, couch_doc:rev_to_str(DDoc1#doc.revs)}
        ]}),
        {ok, _} = couch_db:update_doc(Db, DDocJson11, []),

        %% assert that previously running indexes are gone
        IndsAfter = get_indexes_by_ddoc(Db#db.name, ?DDOCID1, 0),
        ?assertEqual(0, length(IndsAfter)),
        AliveAfter = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(0, length(AliveAfter))
    end).


should_not_stop_indexes_on_delete_multiple_ddoc(Db0) ->
    ?_test(begin
        % create DDOC1 and DDOC2 with the same Sig
        {ok, _} = couch_db:update_docs(Db0, [?DDOC1, ?DDOC2], []),
        {ok, Db} = couch_db:reopen(Db0),
        {ok, DDoc1} = couch_db:open_doc(
            Db, ?DDOCID1, [ejson_body, ?ADMIN_CTX]),
        {ok, DDoc2} = couch_db:open_doc(
            Db, ?DDOCID2, [ejson_body, ?ADMIN_CTX]),

        dreyfus_rpc:call(
            search, Db#db.name, DDoc1, ?IDXNAME, #index_query_args{}),
        dreyfus_rpc:call(
            search, Db#db.name, DDoc2, ?IDXNAME, #index_query_args{}),
        IndsBefore = get_indexes_by_ddoc(Db#db.name, ?DDOCID1, 1),
        ?assertEqual(1, length(IndsBefore)),
        AliveBefore = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(1, length(AliveBefore)),

        % delete DDoc1
        DDocJson11 = couch_doc:from_json_obj({[
           {<<"_id">>, ?DDOCID1},
           {<<"_deleted">>, true},
           {<<"_rev">>, couch_doc:rev_to_str(DDoc1#doc.revs)}
        ]}),
        {ok, _} = couch_db:update_doc(Db, DDocJson11, []),

        %% assert that previously running indexes are still there
        IndsAfter = get_indexes_by_ddoc(Db#db.name, ?DDOCID1, 1),
        ?assertEqual(1, length(IndsAfter)),
        AliveAfter = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(1, length(AliveAfter))
    end).


should_stop_indexes_on_update(Db0) ->
     ?_test(begin
        {ok, _} = couch_db:update_docs(Db0, [?DDOC3], []),
        {ok, Db} = couch_db:reopen(Db0),
        {ok, DDoc3} = couch_db:open_doc(
            Db, ?DDOCID3, [ejson_body, ?ADMIN_CTX]),

        dreyfus_rpc:call(
            search, Db#db.name, DDoc3, ?IDXNAME, #index_query_args{}),
        dreyfus_rpc:call(
            search, Db#db.name, DDoc3, ?IDXNAME2, #index_query_args{}),
        IndsBefore = get_indexes_by_ddoc(Db#db.name, ?DDOCID3, 2),
        ?assertEqual(2, length(IndsBefore)),
        AliveBefore = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(2, length(AliveBefore)),

        % update <<"index1">> of DDoc3
        DDocJson31 = couch_doc:from_json_obj({[
           {<<"_id">>, ?DDOCID3},
           {<<"indexes">>, ?DDOC31IND},
           {<<"_rev">>, couch_doc:rev_to_str(DDoc3#doc.revs)}
        ]}),
        {ok, _} = couch_db:update_doc(Db, DDocJson31, []),

        %% assert that one index process is gone (for <<"index1">>),
        %% and one is still running (for <<"index2>>")
        IndsAfter = get_indexes_by_ddoc(Db#db.name, ?DDOCID3, 1),
        ?assertEqual(1, length(IndsAfter)),
        AliveAfter = lists:filter(fun erlang:is_process_alive/1, IndsBefore),
        ?assertEqual(1, length(AliveAfter))
    end).


fake_rexi() ->
    meck:new([rexi]),
    meck:expect(rexi, reply, fun(Msg) -> Msg end).


fake_clouseau() ->
    ok = meck:new([clouseau_rpc], [non_strict]),
    ok = meck:expect(clouseau_rpc, open_index, ['_', '_', '_'], {ok, self()}),
    ok = meck:expect(clouseau_rpc, get_update_seq, ['_'], {ok, 10}).


fake_dreyfus_index() ->
    ok = meck:new([dreyfus_index], [passthrough]),
    ok = meck:expect(dreyfus_index, await, ['_', '_'], {ok, 0, 0}),
    ok = meck:expect(dreyfus_index, search, ['_', '_'],
        {ok, #top_docs{
        update_seq = 10,
        total_hits = 0,
        hits = []}}).


get_indexes_by_ddoc(DbName, DDocID, N) ->
    Indexes = test_util:wait(fun() ->
        Idxs = ets:match_object(
            dreyfus_by_db, {DbName, {DDocID, '_'}}),
        case length(Idxs) == N of
            true ->
                Idxs;
            false ->
                wait
        end
    end),
    lists:foldl(fun({DBName, {_DDocID, Sig}}, Acc) ->
        case ets:lookup(dreyfus_by_sig, {DBName, Sig}) of
            [{_, Pid}] -> [Pid|Acc];
            _ -> Acc
        end
    end, [], Indexes).
