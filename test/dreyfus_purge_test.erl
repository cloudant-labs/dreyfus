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

-module(dreyfus_purge_test).

-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include_lib("couch/include/couch_eunit.hrl").

-export([test_purge_single/0, test_purge_multiple/0, test_purge_multiple2/0, test_purge_conflict/0, test_all/0]).
%-export([create_db_docs/1, create_docs/2, delete_db/1, purge_one_doc/2, dreyfus_search/2]).

test_all() ->
    test_purge_single(),
    test_purge_multiple(),
    test_purge_multiple2(),
    test_purge_conflict(),
    ok.


test_purge_single() ->
    DbName = db_name(),
    create_db_docs(DbName),
    {ok, _, HitCount1, _, _, _} = dreyfus_search(DbName, <<"apple">>),
    ?assertEqual(HitCount1, 1),
    DBFullName = get_full_dbname(DbName),
    ok = purge_one_doc(DBFullName, <<"apple">>),
    {ok, _, HitCount2, _, _, _} = dreyfus_search(DbName, <<"apple">>),
    ?assertEqual(HitCount2, 0),
    delete_db(DbName),
    ok.


test_purge_multiple() ->
    Query = <<"color:red">>,

    %create the db and docs
    DbName = db_name(),
    create_db_docs(DbName),

    %first search request
    {ok, _, HitCount1, _, _, _} = dreyfus_search(DbName, Query),

    ?assertEqual(HitCount1, 5),

    %get full dbname
    DBFullName = get_full_dbname(DbName),

    %purge 5 docs
    purge_docs(DBFullName, [<<"apple">>, <<"tomato">>, <<"cherry">>, <<"haw">>, <<"strawberry">>]),

    %second search request
    {ok, _, HitCount2, _, _, _} = dreyfus_search(DbName, Query),

    ?assertEqual(HitCount2, 0),

    %delete the db
    delete_db(DbName),
    ok.


test_purge_multiple2() ->
    %create the db and docs
    DbName = db_name(),
    create_db_docs(DbName),

    Query = <<"color:red">>,

    %first search request
    {ok, _, HitCount1, _, _, _} = dreyfus_search(DbName, Query),

    ?assertEqual(HitCount1, 5),

    DBFullName = get_full_dbname(DbName),

    %purge 2 docs
    purge_docs(DBFullName, [<<"apple">>, <<"tomato">>]),

    %second search request
    {ok, _, HitCount2, _, _, _} = dreyfus_search(DbName, Query),

    ?assertEqual(HitCount2, 3),

    %purge 2 docs
    purge_docs(DBFullName, [<<"cherry">>, <<"haw">>]),

    %third search request
    {ok, _, HitCount3, _, _, _} = dreyfus_search(DbName, Query),

    ?assertEqual(HitCount3, 1),

    %delete the db
    delete_db(DbName),
    ok.

test_purge_conflict() ->
    %create dbs and docs
    SourceDbName = db_name(),
    timer:sleep(2000),
    TargetDbName = db_name(),

    create_db_docs(SourceDbName),
    create_db(TargetDbName),
    GreenDocs = [make_doc_green(I) || I <- lists:seq(1, 5)],
    {ok, _} = fabric:update_docs(TargetDbName, GreenDocs, [?ADMIN_CTX]),
    {ok, _} = fabric:update_doc(TargetDbName, make_design_doc(dreyfus), [?ADMIN_CTX]),

    %first search
    {ok, _, RedHitCount1, _RedHits1, _, _} = dreyfus_search(TargetDbName, <<"color:red">>),
    {ok, _, GreenHitCount1, _GreenHits1, _, _} = dreyfus_search(TargetDbName, <<"color:green">>),

    ?assertEqual(5, RedHitCount1 + GreenHitCount1),

    %do replicate and make conflicted docs
    {ok, _} = fabric:update_doc(<<"_replicator">>, make_replicate_doc(SourceDbName, TargetDbName), [?ADMIN_CTX]),

    timer:sleep(5000),

    %second search
    {ok, _, RedHitCount2, _RedHits2, _, _} = dreyfus_search(TargetDbName, <<"color:red">>),
    {ok, _, GreenHitCount2, _GreenHits2, _, _} = dreyfus_search(TargetDbName, <<"color:green">>),

    ?assertEqual(5, RedHitCount2 + GreenHitCount2),

    DBFullName = get_full_dbname(TargetDbName),
    purge_docs(DBFullName, [<<"apple">>, <<"tomato">>, <<"cherry">>, <<"haw">>, <<"strawberry">>]),

    %third search
    {ok, _, RedHitCount3, _RedHits3, _, _} = dreyfus_search(TargetDbName, <<"color:red">>),
    {ok, _, GreenHitCount3, _GreenHits3, _, _} = dreyfus_search(TargetDbName, <<"color:green">>),

    ?assertEqual(5, RedHitCount3 + GreenHitCount3),
    ?assertEqual(RedHitCount2, GreenHitCount3),
    ?assertEqual(GreenHitCount2, RedHitCount3),

    delete_db(SourceDbName),
    delete_db(TargetDbName),
    ok.

%private API
db_name() ->
    Nums = tuple_to_list(erlang:now()),
    Prefix = "test-db",
    Suffix = lists:concat([integer_to_list(Num) || Num <- Nums]),
    list_to_binary(Prefix ++ "-" ++ Suffix).

purge_one_doc(DBFullName, DocId) ->
    {ok, Db} = couch_db:open_int(DBFullName, []),
    FDI = couch_db:get_full_doc_info(Db, DocId),
    #doc_info{ revs = [#rev_info{} = PrevRev | _] } = couch_doc:to_doc_info(FDI),
    Rev = PrevRev#rev_info.rev,
    couch_log:notice("[~p] purge doc Id:~p, Rev:~p", [?MODULE, DocId, Rev]),
    {ok, {_, [{ok, _}]}} = couch_db:purge_docs(Db, [{DocId, [Rev]}]),
    ok.

purge_docs(DBFullName, DocIds) ->
    lists:foreach(
        fun(DocId) -> ok = purge_one_doc(DBFullName, DocId) end,
        DocIds
    ).

dreyfus_search(DbName, KeyWord) ->
    QueryArgs = #index_query_args{q = KeyWord},
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/search">>, []),
    dreyfus_fabric_search:go(DbName, DDoc, <<"index">>, QueryArgs).

create_db_docs(DbName) ->
    create_db(DbName),
    create_docs(DbName, 5).

create_docs(DbName, Count) ->
    {ok, _} = fabric:update_docs(DbName, make_docs(Count), [?ADMIN_CTX]),
    {ok, _} = fabric:update_doc(DbName, make_design_doc(dreyfus), [?ADMIN_CTX]).

create_db(DbName) ->
    ok = fabric:create_db(DbName, [?ADMIN_CTX]).

delete_db(DbName) ->
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).

make_docs(Count) ->
    [make_doc(I) || I <- lists:seq(1, Count)].

make_doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, get_value(Id)},
        {<<"color">>, <<"red">>},
        {<<"version">>, Id}
    ]}).

make_doc_green(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, get_value(Id)},
        {<<"color">>, <<"green">>},
        {<<"version">>, Id}
    ]}).

get_value(Key) ->
    case Key of
        1 -> <<"apple">>;
        2 -> <<"tomato">>;
        3 -> <<"cherry">>;
        4 -> <<"strawberry">>;
        5 -> <<"haw">>;
        6 -> <<"carrot">>;
        7 -> <<"pitaya">>;
        8 -> <<"grape">>;
        9 -> <<"date">>;
        10 -> <<"watermelon">>
    end.

get_full_dbname(DbName) ->
    Suffix = lists:sublist(binary_to_list(DbName), 9, 10),
    %Suffix1 = lists:sublist(binary_to_list(DbName), 9, 4),
    %Suffix2 = lists:sublist(binary_to_list(DbName), 13, 5),
    %Suffix = Suffix1 ++ "0" ++ Suffix2,
    DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
    DBFullName.

make_design_doc(dreyfus) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/search">>},
        {<<"language">>, <<"javascript">>},
        {<<"indexes">>, {[
            {<<"index">>, {[
                {<<"analyzer">>, <<"standard">>},
                {<<"index">>, <<
                    "function (doc) { \n"
                    "  index(\"default\", doc._id);\n"
                    "  if(doc.color) {\n"
                    "    index(\"color\", doc.color);\n"
                    "  }\n"
                    "  if(doc.version) {\n"
                    "    index(\"version\", doc.version);\n"
                    "  }\n"
                    "}"
                >>}
            ]}}
        ]}}
    ]}).

make_replicate_doc(SourceDbName, TargetDbName) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary("replicate_doc" ++ TargetDbName)},
        {<<"source">>, list_to_binary("http://localhost:15984/" ++ SourceDbName)},
        {<<"target">>, list_to_binary("http://localhost:15984/" ++ TargetDbName)}
    ]}).
