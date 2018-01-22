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

-module(dreyfus_httpc).

-include_lib("couch/include/couch_db.hrl").
-include("dreyfus.hrl").

-export([setup/0]).
-export([search_req/2]).

-define(MAX_CONNECTIONS, 10).
-define(CLOUSEAU_HOST, "localhost").
-define(CLOUSEAU_PORT1, 15985).
-define(CLOUSEAU_PORT2, 15986).
-define(CLOUSEAU_PORT3, 15987).
-define(CLOUSEAU_URL,
    "http://" ++ ?CLOUSEAU_HOST ++ ":").


setup()->
    ibrowse:set_max_sessions(?CLOUSEAU_HOST, ?CLOUSEAU_PORT1, ?MAX_CONNECTIONS),
    ibrowse:set_max_sessions(?CLOUSEAU_HOST, ?CLOUSEAU_PORT2, ?MAX_CONNECTIONS),
    ibrowse:set_max_sessions(?CLOUSEAU_HOST, ?CLOUSEAU_PORT3, ?MAX_CONNECTIONS).


search_req(Path0, Query) ->
    Path = ?b2l(base64:encode(Path0)),
    Port = case node() of
        'node1@127.0.0.1' ->
            ?CLOUSEAU_PORT1;
        'node2@127.0.0.1' ->
            ?CLOUSEAU_PORT2;
        'node3@127.0.0.1' ->
            ?CLOUSEAU_PORT3
    end,
    Url = ?CLOUSEAU_URL ++ integer_to_list(Port) ++"/index/" ++ Path ++ "/search?q="++ ?b2l(Query),
    couch_log:notice("Node ~p, URL ~p, Port ~p", [node(), Url, Port]), 
    Resp = ibrowse:send_req(Url, [], get),
    % couch_log:notice("Resp from req resp ~p", [Resp]),
    process_response(Resp).

process_response({ok, Code, _Headers, Body}) ->
    case list_to_integer(Code) of
        Ok when (Ok >= 200 andalso Ok < 300)  ->
            case Body of
                <<>> ->
                    null;
                Json ->
                    {Decode} = ?JSON_DECODE(Json),
                    {ok, #top_docs{
                        update_seq = couch_util:get_value(<<"update_seq">>, Decode),
                        total_hits = couch_util:get_value(<<"total_hits">>, Decode),
                        hits = couch_util:get_value(<<"hit">>, Decode),
                        counts = couch_util:get_value(<<"counts">>, Decode),
                        ranges = couch_util:get_value(<<"ranges">>, Decode)
                    }}
            end;
        Error ->
            Error
    end.