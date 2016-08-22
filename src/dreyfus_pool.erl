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

-module(dreyfus_pool).
-behaviour(gen_server).
-vsn(1).

-export([
    start_link/0,
    get_socket/0,
    close_socket/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-record(state, {
        host,
        port,
        num_sockets,
        pool = [],
        index = 1
}).

start_link() ->
    ClouseauNode = [clouseau_host(), clouseau_port(),clouseau_socket_count()],
    gen_server:start_link({local, ?MODULE}, ?MODULE, ClouseauNode, []).

get_socket() ->
    gen_server:call(?MODULE, {get_socket}, infinity).

close_socket(Socket, Requests) ->
    gen_server:cast(?MODULE, {close_socket, Socket, Requests}).

init([Host, Port,SocketCount]) ->
    process_flag(trap_exit, true),
    {ok, #state{host=Host, port=Port,num_sockets = SocketCount}}.

handle_call({get_socket}, _From, #state{pool=Pool, index=Index,
    num_sockets=Max} = State) ->
    case length(Pool) < Max of
    true ->
        SocketPid = spawn_link(clouseau_tcp_pool, connect, [State#state.host,
                State#state.port]),
        NewPool = [SocketPid|Pool],
        {reply, {ok, SocketPid}, State#state{pool = NewPool}};
    false ->
        case Index < Max of
            true ->
                NewIndex = Index + 1;
            false ->
                NewIndex = 1
        end,
        SocketPid = lists:nth(NewIndex,Pool),
        {reply, {ok, SocketPid}, State#state{index = NewIndex}}
    end.

handle_cast({close_socket, SocketPid, Requests}, #state{pool=Pool}=State) ->
    NewPool1 = lists:filter(fun(Pid) -> Pid /= SocketPid end,  Pool),
    %get a new socket and transfer to it requests from the closed socket
    NewSocketPid = spawn_link(clouseau_tcp_pool, connect, [State#state.host,
            State#state.port, Requests]),
    NewPool = [NewSocketPid|NewPool1],
    {noreply, State#state{pool=NewPool}}.

handle_info({'EXIT', FromPid, Reason}, State) ->
    couch_log:debug("$$$$$$$$ In handelinfo Exit and the Reason for exit is ~p",
        [Reason]),
    dreyfus_pool:close_socket(FromPid, maps:new()),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

clouseau_port() ->
    list_to_integer(config:get("dreyfus", "port", "5985")).

clouseau_socket_count() ->
    list_to_integer(config:get("dreyfus", "num_sockets", "24")).

clouseau_host() ->
    list_to_atom(config:get("dreyfus", "host", "localhost")).