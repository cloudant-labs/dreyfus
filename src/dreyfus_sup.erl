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


%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-

-module(dreyfus_sup).
-behaviour(supervisor).
-behaviour(config_listener).

-export([start_link/0, init/1]).

-export([handle_config_change/5, handle_config_terminate/3]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    Children = [
        child(dreyfus_index_manager),
        {
            config_listener_mon,
            {config_listener_mon, start_link, [?MODULE, nil]},
            permanent,
            5000,
            worker,
            [config_listener_mon]
        }
    ],
    {ok, {{one_for_one,10,1},
        couch_epi:register_service(dreyfus_epi, Children)}}.

child(Child) ->
    {Child, {Child, start_link, []}, permanent, 1000, worker, [Child]}.

handle_config_change("dreyfus", Key, _, _, S) ->
    case Key of
        "black_list" ->
            dreyfus_config:reconfigure();
        _ ->
            % Someone may have changed the config for
            % the writer so we need to re-initialize.
            dreyfus_config:reconfigure()
    end,
    notify_listeners(),
    {ok, S};
handle_config_change(_, _, _, _, S) ->
    {ok, S}.
handle_config_terminate(_Server, _Reason, _State) ->
    ok.


-ifdef(TEST).
notify_listeners() ->
    Listeners = application:get_env(dreyfus, config_listeners, []),
    lists:foreach(fun(L) ->
        L ! dreyfus_config_change_finished
    end, Listeners).
-else.
notify_listeners() ->
    ok.
-endif.
