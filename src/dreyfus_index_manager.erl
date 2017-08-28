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

-module(dreyfus_index_manager).
-behaviour(gen_server).
-vsn(1).
-include_lib("couch/include/couch_db.hrl").
-include("dreyfus.hrl").

-define(BY_SIG, dreyfus_by_sig).
-define(BY_PID, dreyfus_by_pid).
-define(BY_DB, dreyfus_by_db).

% public api.
-export([start_link/0, get_index/2, get_disk_size/2]).

% gen_server api.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export([handle_db_event/3]).

% public functions.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_index(DbName, Index) ->
    gen_server:call(?MODULE, {get_index, DbName, Index}, infinity).

get_disk_size(DbName, Index) ->
    gen_server:call(?MODULE, {get_disk_size, DbName, Index}, infinity).

% gen_server functions.

init([]) ->
    ets:new(?BY_SIG, [set, protected, named_table]),
    ets:new(?BY_PID, [set, private, named_table]),
    ets:new(?BY_DB, [bag, protected, named_table]),
    couch_event:link_listener(?MODULE, handle_db_event, nil, [all_dbs]),
    process_flag(trap_exit, true),
    {ok, nil}.

handle_call({get_index, DbName, #index{sig=Sig}=Index}, From, State) ->
    case ets:lookup(?BY_SIG, {DbName, Sig}) of
    [] ->
        Pid = spawn_link(fun() -> new_index(DbName, Index) end),
        ets:insert(?BY_PID, {Pid, opening, {DbName, Sig}}),
        ets:insert(?BY_SIG, {{DbName,Sig}, [From]}),
        {noreply, State};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(?BY_SIG, {{DbName, Sig}, [From | WaitList]}),
        {noreply, State};
    [{_, ExistingPid}] ->
        DDocId = Index#index.ddoc_id,
        ets:insert(?BY_DB, {DbName, {DDocId, Sig}}),
        {reply, {ok, ExistingPid}, State}
    end;

handle_call({get_disk_size, DbName, #index{sig=Sig}}, _From, State) ->
    Path = <<DbName/binary,"/",Sig/binary>>,
    Reply = clouseau_rpc:disk_size(Path),
    {reply, Reply, State};

handle_call({open_ok, DbName, DDocId, Sig, NewPid}, {OpenerPid, _}, State) ->
    link(NewPid),
    [{_, WaitList}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    [gen_server:reply(From, {ok, NewPid}) || From <- WaitList],
    ets:delete(?BY_PID, OpenerPid),
    add_to_ets(NewPid, DbName, DDocId, Sig),
    {reply, ok, State};

handle_call({open_error, DbName, Sig, Error}, {OpenerPid, _}, State) ->
    [{_, WaitList}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    [gen_server:reply(From, Error) || From <- WaitList],
    ets:delete(?BY_PID, OpenerPid),
    ets:delete(?BY_SIG, {DbName, Sig}),
    {reply, ok, State}.

handle_cast({cleanup, DbName}, State) ->
    clouseau_rpc:cleanup(DbName),
    {noreply, State};
handle_cast({rem_from_ets, [DbName, DDocId, Sig]}, State) ->
    ets:delete_object(?BY_DB, {DbName, {DDocId, Sig}}),
    {noreply, State}.

handle_info({'EXIT', FromPid, Reason}, State) ->
    case ets:lookup(?BY_PID, FromPid) of
    [] ->
        if Reason =/= normal ->
            couch_log:error("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    % Using Reason /= normal to force a match error
    % if we didn't delete the Pid in a handle_call
    % message for some reason.
    [{_, opening, {DbName, Sig}}] when Reason /= normal ->
        Msg = {open_error, DbName, Sig, Reason},
        {reply, ok, _} = handle_call(Msg, {FromPid, nil}, State);
    [{_, {DbName, Sig}}] ->
        delete_from_ets(FromPid, DbName, Sig)
    end,
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, nil, _Extra) ->
    {ok, nil}.

% private functions

handle_db_event(DbName, created, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName}),
    {ok, nil};
handle_db_event(DbName, deleted, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName}),
    {ok, nil};
handle_db_event(<<"shards/", _/binary>> = DbName, {ddoc_updated,
        DDocId}, _St) ->
    DDocResult = couch_util:with_db(DbName, fun(Db) ->
        couch_db:open_doc(Db, DDocId, [ejson_body, ?ADMIN_CTX])
    end),
    DbShards = [mem3:name(Sh) || Sh <- mem3:local_shards(mem3:dbname(DbName))],
    lists:foreach(fun(DbShard) ->
        lists:foreach(fun({_DbShard, {_DDocId, Sig}}) ->
            % check if there are other ddocs with the same Sig for the same db
            SigDDocs = ets:match_object(?BY_DB, {DbShard, {'_', Sig}}),
            if length(SigDDocs) > 1 ->
                % remove a record from this DDoc from ?BY_DB
                Args = [DbShard, DDocId, Sig],
                gen_server:cast(?MODULE, {rem_from_ets, Args});
            true ->
                % single DDoc with this Sig - maybe close dreyfus_index process
                case ets:lookup(?BY_SIG, {DbShard, Sig}) of
                    [{_, IndexPid}] -> (catch
                        gen_server:cast(IndexPid, {ddoc_updated, DDocResult}));
                    [] -> []
                end
            end
        end, ets:match_object(?BY_DB, {DbShard, {DDocId, '_'}}))
    end, DbShards),
    {ok, nil};
handle_db_event(DbName, {ddoc_updated, DDocId}, St) ->
    DDocResult = couch_util:with_db(DbName, fun(Db) ->
        couch_db:open_doc(Db, DDocId, [ejson_body, ?ADMIN_CTX])
    end),
    lists:foreach(fun({_DbName, {_DDocId, Sig}}) ->
        SigDDocs = ets:match_object(?BY_DB, {DbName, {'_', Sig}}),
        if length(SigDDocs) > 1 ->
            Args = [DbName, DDocId, Sig],
            gen_server:cast(?MODULE, {rem_from_ets, Args});
        true ->
            case ets:lookup(?BY_SIG, {DbName, Sig}) of
                [{_, IndexPid}] -> (catch
                    gen_server:cast(IndexPid, {ddoc_updated, DDocResult}));
                [] -> []
            end
        end
    end, ets:match_object(?BY_DB, {DbName, {DDocId, '_'}})),
    {ok, St};
handle_db_event(_DbName, _Event, St) ->
    {ok, St}.


new_index(DbName, #index{ddoc_id=DDocId, sig=Sig}=Index) ->
    case (catch dreyfus_index:start_link(DbName, Index)) of
    {ok, NewPid} ->
        Msg = {open_ok, DbName, DDocId, Sig, NewPid},
        ok = gen_server:call(?MODULE, Msg, infinity),
        unlink(NewPid);
    Error ->
        Msg = {open_error, DbName, Sig, Error},
        ok = gen_server:call(?MODULE, Msg, infinity)
    end.

add_to_ets(Pid, DbName, DDocId, Sig) ->
    true = ets:insert(?BY_PID, {Pid, {DbName, Sig}}),
    true = ets:insert(?BY_SIG, {{DbName, Sig}, Pid}),
    true = ets:insert(?BY_DB, {DbName, {DDocId, Sig}}).

delete_from_ets(Pid, DbName, Sig) ->
    true = ets:match_delete(?BY_DB, {DbName, {'_', Sig}}),
    true = ets:delete(?BY_PID, Pid),
    true = ets:delete(?BY_SIG, {DbName, Sig}).

