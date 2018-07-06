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
                    fun check_restart_listener/0,
                    fun check_black_list/0,
                    fun check_add_to_black_list/0,
                    fun check_delete_from_blacklist/0
                ]
            }
        }
    }.

check_restart_listener() ->
    Listener1 = dreyfus_test_util:get_listener(),
    ?assert(is_process_alive(Listener1)),

    Handler1 = dreyfus_test_util:get_handler(),
    ?assertNotEqual(not_found, Handler1),
    Ref = erlang:monitor(process, Listener1),
    ok = gen_event:delete_handler(config_event, dreyfus_test_util:get_handler(),
        testing),

    receive
        {'DOWN', Ref, process, _, _} ->
            ?assertNot(is_process_alive(Listener1))
        after ?TIMEOUT ->
            erlang:error({timeout, config_listener_mon_death})
    end,

    NewHandler = test_util:wait(fun() ->
        case dreyfus_test_util:get_handler() of
            not_found -> wait;
            Reply -> Reply
        end
    end, ?TIMEOUT, 20),
    ?assertEqual(Handler1, NewHandler),

    Listener2 = dreyfus_test_util:get_listener(),
    ?assert(is_process_alive(Listener2)),
    ?assertNotEqual(Listener1, Listener2),
    ok.


check_black_list() ->
    Index = ["mydb", "myddocid", "myindexname"],
    dreyfus_test_util:with_config_listener(fun() ->
        config:set("dreyfus", "black_list", [Index]),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual([Index], dreyfus_config:get(black_list))
    end).

check_add_to_black_list() ->
    Index = ["mydb", "myddocid", "myindexname"],
    Index2 = ["mydb2", "myddocid2", "myindexname2"],
    Index3 = ["mydb3", "myddocid3", "myindexname3"],
    Index4 = {"mydb4", "myddocid4", "myindexname4"},
    dreyfus_test_util:with_config_listener(fun() ->
        dreyfus_util:add_bl_element(<<"mydb">>, <<"myddocid">>, <<"myindexname">>),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:add_bl_element("mydb2", "myddocid2", "myindexname2"),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:add_bl_element(Index3),
        dreyfus_test_util:wait_for_config(),
        % don't wait for config here because config doesn't get changed
        dreyfus_util:add_bl_element(Index4),
        FinalBl = [Index3, Index2, Index],
        ?assertEqual(FinalBl, dreyfus_config:get(black_list))
    end).

check_delete_from_blacklist() ->
    Index = ["mydb", "myddocid", "myindexname"],
    Index2 = ["mydb2", "myddocid2", "myindexname2"],
    Index3 = ["mydb3", "myddocid3", "myindexname3"],
    dreyfus_test_util:with_config_listener(fun() ->
        dreyfus_util:add_bl_element(Index),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:add_bl_element(Index2),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:add_bl_element(Index3),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:remove_bl_element(<<"mydb">>, <<"myddocid">>,
            <<"myindexname">>),
        dreyfus_test_util:wait_for_config(),
        dreyfus_util:remove_bl_element("mydb2", "myddocid2", "myindexname2"),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual([Index3], dreyfus_config:get(black_list)),
        dreyfus_util:remove_bl_element(Index3),
        dreyfus_test_util:wait_for_config(),
        ?assertEqual([], dreyfus_config:get(black_list))
    end).
