% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(dreyfus_config_test).


-include_lib("couch_log/include/couch_log.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 1000).


start() ->
    test_util:start_couch([dreyfus]).

setup() ->
    config:set("dreyfus", "black_list", []).

teardown(_) ->
    ok.

dreyfus_config_test_() ->
    {
        "dreyfus config tests",
        {
            setup,
            fun start/0, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun check_black_list/0,
                    fun check_add_to_black_list/0,
                    fun check_delete_from_blacklist/0
                ]
            }
        }
    }.


check_black_list() ->
    Index = ["mydb", "myddocid", "myindexname"],
    dreyfus_test_util:with_config_listener(fun() ->
        config:set("dreyfus", "black_list", [Index]),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual(true, dreyfus_config:get(Index))
    end).

check_add_to_black_list() ->
    Index = ["mydb", "myddocid", "myindexname"],
    Index2 = ["mydb2", "myddocid2", "myindexname2"],
    Index3 = ["mydb3", "myddocid3", "myindexname3"],
    Index4 = {"mydb4", "myddocid4", "myindexname4"},
    dreyfus_test_util:with_config_listener(fun() ->
        dreyfus_test_util:add_bl_element(<<"mydb">>, <<"myddocid">>, <<"myindexname">>),
        dreyfus_test_util:add_bl_element("mydb2", "myddocid2", "myindexname2"),
        dreyfus_test_util:add_bl_element(Index3),
        dreyfus_test_util:wait_for_config(),
        dreyfus_test_util:add_bl_element(Index4),
        FinalBl = [Index3, Index2, Index],
        lists:foreach(fun (I) ->
            ?assertEqual(true, dreyfus_config:get(I))
        end, FinalBl)
    end).

check_delete_from_blacklist() ->
    Index = ["mydb", "myddocid", "myindexname"],
    Index2 = ["mydb2", "myddocid2", "myindexname2"],
    Index3 = ["mydb3", "myddocid3", "myindexname3"],
    dreyfus_test_util:with_config_listener(fun() ->
        dreyfus_test_util:add_bl_element(Index),
        dreyfus_test_util:add_bl_element(Index2),
        dreyfus_test_util:add_bl_element(Index3),
        dreyfus_test_util:remove_bl_element(<<"mydb">>, <<"myddocid">>,
            <<"myindexname">>),
        dreyfus_test_util:remove_bl_element("mydb2", "myddocid2", "myindexname2"),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual(undefined, dreyfus_config:get(Index)),
        ?assertEqual(undefined, dreyfus_config:get(Index2)),
        ?assertEqual(true, dreyfus_config:get(Index3)),
        dreyfus_test_util:remove_bl_element(Index3),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual(undefined, dreyfus_config:get(Index3))
    end).
