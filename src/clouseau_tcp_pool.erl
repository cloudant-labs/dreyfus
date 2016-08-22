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

-module(clouseau_tcp_pool).

-include("dreyfus.hrl").

-export([commit/4, info/0, search/2, group1/7, group2/8, multi_shard_search/4]).
-export([delete/5, update/7, cleanup/1, cleanup/2]).
-export([analyze/2, version/0]).
-export([connect/2, connect/3, loop/2]).

connect(Host, Port) ->
    connect(Host, Port, maps:new()).

connect(Host, Port, Requests) ->
    Options = [{active, 5},
               {mode, binary},
               {nodelay, true},
               {packet, 4}],
    case gen_tcp:connect(Host,
                         Port,
                         Options) of
        {ok, Socket} ->
            loop(Socket, Requests);

        {error,Error} ->
            {error,Error}
    end.


loop(Socket, Requests) ->
    receive
        {send, From, PacketIn} ->
            case gen_tcp:send(Socket, PacketIn) of
                ok ->
                    NewRequests = maps:put(From, os:timestamp(), Requests),
                    loop(Socket, NewRequests);
                {error, Reason} ->
                    couch_log:debug("Error sending ~p",[From]),
                    From!{error, self(), Reason},
                    close(Socket, Requests)
            end;
        {tcp, Socket, Data} ->
            {From, PacketOut} = decode(Data),
            case maps:get(From,Requests) of
                {badkey,_Key} ->
                    couch_log:debug("Request from ~p doesn't exist in Requests map",[From]);
                Start ->
                    Length = timer:now_diff(os:timestamp(), Start) / 1000,
                    case Length > 500 of
                        true ->
                            couch_log:warning("************** It took more than 500 ms for the socket to receive data for pid ~p is ~p *********", [From,Length]);
                        false ->
                            couch_log:debug("Time took for socket to receiece data is ~p",[Length])
                    end
            end,
            couch_log:debug("Received the response for request from ~p",[From]),
            From!{ok, self(), PacketOut},
            NewRequests = maps:remove(From,Requests),
            loop(Socket, NewRequests);
        {tcp_passive, Socket} ->
            inet:setopts(Socket,[{active, 5}]),
            loop(Socket, Requests);
        {tcp_error, Socket} ->
            couch_log:warning("Error in socket because of tcp_error ~p",[Socket]),
            close(Socket, Requests);
        {tcp_closed, Socket} ->
            couch_log:warning("Received  tcp_closed ~p",[Socket]),
            close(Socket, Requests)
    end.

decode(Binary) ->
    Response  = binary_to_term(Binary, []),
    couch_log:debug("Got the response back and the decoded response is ~p",[Response]),
    Response.

close(Socket, Requests) ->
    dreyfus_pool:close_socket(self(), Requests),
    gen_tcp:close(Socket).

commit(DbName, Sig, Analyzer, NewCommitSeq) ->
    Path = <<DbName/binary,"/",Sig/binary>>,
    Command = [{"reqType", <<4>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"newcommitseq", NewCommitSeq}],
    couch_log:warning("$$$$ Sending commit message ~p $$$",[Command]),
    case dreyfus_util:send_clouseau(Command) of
        {ok, _CurSeq} ->
            ok;
        Error ->
            Error
    end.

info() ->
    {ok}.

search(Index, Args) ->
    couch_log:debug("**** In search Index is ~p and query args ***~p ",[Index,Args]),
    #index{analyzer=Analyzer, dbname=DbName, current_seq = Seq, sig=Sig}=Index,
    couch_log:debug("dbname=DbName, sig=Sig are ~p and ~p",[DbName, Sig]),
    Path = <<DbName/binary,"/",Sig/binary>>,
    Command = [{"reqType", <<1>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"seq", Seq},
               {"options", Args}],

    couch_log:debug("++++ Search command is +++++ ~p",[Command]),

    dreyfus_util:send_clouseau(Command).

group1(Index, Query, GroupBy, _Stale , Sort,
       Offset, Limit) ->
    #index{analyzer=Analyzer, current_seq = Seq, dbname=DbName, sig=Sig}=Index,

    Options = [{"q", Query}, {"limit", Limit}, {"sort", Sort}, {"group_by", GroupBy}, {"offset", Offset}],
    couch_log:debug("dbname=DbName, sig=Sig are ~p and ~p",[DbName, Sig]),

    Path = <<DbName/binary,"/",Sig/binary>>,
    Command = [{"reqType", <<6>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"seq", Seq},
               {"options", Options}],

    couch_log:debug("++++ Group1 Command is +++++ ~p",[Command]),

    dreyfus_util:send_clouseau(Command).

group2(Index, Query, GroupBy, _Stale, Groups,
                                GroupSort, DocSort, DocLimit) ->
    #index{analyzer=Analyzer, current_seq = Seq, dbname=DbName, sig=Sig}=Index,
    Options = [{"q", Query}, {"limit", DocLimit}, {"docsort", DocSort},
               {"groupsort", GroupSort}, {"group_by", GroupBy}, {"groups", Groups}],
    couch_log:debug("dbname=DbName, sig=Sig are ~p and ~p",[DbName, Sig]),

    Path = <<DbName/binary,"/",Sig/binary>>,
    Command = [{"reqType", <<7>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"seq", Seq},
               {"options", Options}],

    couch_log:debug("++++ Group2 Command is +++++ ~p",[Command]),

    dreyfus_util:send_clouseau(Command).

multi_shard_search(DbName, Paths, Index, QueryArgs) ->
    couch_log:debug("**** In search Index is ~p and query args ***~p ",[Index,QueryArgs]),
    #index{analyzer=Analyzer, sig=Sig}=Index,
    #index_query_args{
                      q = Query,
                      limit = Limit,
                      curr_seq=Seq
                     } = QueryArgs,
    Options = [{"q",Query}, {"limit" , Limit}],

    couch_log:debug("dbname=DbName, sig=Sig are ~p and ~p",[DbName, Sig]),

    Path = <<DbName/binary,"/",Sig/binary>>,

    Command = [{"reqType", <<0>>},
               {"path", Path},
               {"paths",Paths},
               {"analyzer", Analyzer},
               {"seq", Seq},
               {"options", Options}],

    couch_log:debug("++++ Command is +++++ ~p",[Command]),

    dreyfus_util:send_clouseau(Command).

delete(DbName, Sig, Analyzer,Seq,Id) ->
    couch_log:debug("Delete request => DbName, Sig, Analyzer, Seq, Id are ~p ~p ~p ~p ~p",
      [DbName, Sig, Analyzer, Seq, Id]),
    Path = <<DbName/binary,"/",Sig/binary>>,
    Command = [{"reqType", <<5>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"id", Id},
               {"seq", Seq}],
    ok = dreyfus_util:send_clouseau(Command).

update(DbName, Sig, Analyzer, Seq, Id, Fields,TargetSeq) ->
    Path = <<DbName/binary,"/",Sig/binary>>,
    [Field|_Rest] = Fields,
    couch_log:debug("Update request DbName, Sig, Analyzer, Seq, Id, Field | Fields are ~p ~p ~p ~p ~p ~p",[DbName, Sig, Analyzer, Seq, Id, Field ]),
    Command = [{"reqType", <<2>>},
               {"path", Path},
               {"analyzer", Analyzer},
               {"id", Id},
               {"seq", Seq},
               {"target_seq", TargetSeq},
               {"fields", Fields}],
    couch_log:debug("Sending update request for doc ~p with fields ~p",[Id, Fields]),
    ok = dreyfus_util:send_clouseau(Command).

cleanup(DbName) ->
    Path = <<DbName/binary>>,
    couch_log:debug("Cleanup for DbName ~p",DbName),
    Command = [{"reqType", <<9>>},
               {"path", Path}],
    dreyfus_util:send_clouseau(Command),
    {ok,nil}.

cleanup(DbName, ActiveSigs) ->
    Path = <<DbName/binary>>,
    couch_log:debug("Cleanup for DbName ~p and ActiveSigs  ~p", DbName, ActiveSigs),
    Command = [{"reqType", <<9>>},
               {"path", Path},
               {"active_sigs", ActiveSigs}],
    dreyfus_util:send_clouseau(Command),
    {ok,nil}.

analyze(Analyzer, Text) ->
    Command = [{"reqType", <<10>>},
               {"text", <<Text/binary>>},
               {"analyzer", Analyzer}],
    dreyfus_util:send_clouseau(Command).

version() ->
    Command = [{"reqType", <<0>>}],
    dreyfus_util:send_clouseau(Command).
