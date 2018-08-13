-module(dreyfus_test_util).

-compile(export_all).

-include_lib("couch/include/couch_db.hrl").

add_listener(Listener) ->
    Listeners = case application:get_env(dreyfus, config_listeners) of
        {ok, L} when is_list(L) ->
            lists:usort([Listener | L]);
        _ ->
            [Listener]
    end,
    application:set_env(dreyfus, config_listeners, Listeners).


rem_listener(Listener) ->
    Listeners = case application:get_env(dreyfus, config_listeners) of
        {ok, L} when is_list(L) ->
            L -- [Listener];
        _ ->
            []
    end,
    application:set_env(dreyfus, config_listeners, Listeners).


with_config_listener(Fun) ->
    Listener = self(),
    try
        add_listener(Listener),
        Fun()
    after
        rem_listener(Listener)
    end.


wait_for_config() ->
    receive
        dreyfus_config_change_finished -> ok
    % waiting 2000 here because the polling in dreyfus_config is 1000
    after 2000 ->
        erlang:error(config_change_timeout)
    end.


get_listener() ->
    Children = supervisor:which_children(dreyfus_sup),
    hd([Pid || {config_listener_mon, Pid, _, _} <- Children]).


get_handler() ->
    FoldFun = fun
        ({config_listener, {dreyfus_sup, _}} = H, not_found) ->
            H;
        (_, Acc) ->
            Acc
    end,
    lists:foldl(FoldFun, not_found, gen_event:which_handlers(config_event)).

add_bl_element(DbName, GroupId, IndexName) when is_list(DbName),
        is_list(GroupId), is_list(IndexName) ->
    add_bl_element([DbName, GroupId, IndexName]);
add_bl_element(DbName, GroupId, IndexName) when is_binary(DbName),
        is_binary(GroupId), is_binary(IndexName) ->
    add_bl_element([?b2l(DbName), ?b2l(GroupId), ?b2l(IndexName)]).

add_bl_element(IndexEntry) when is_list(IndexEntry) ->
    BlackList = config:get("dreyfus", "black_list", []),
    UpdatedList = case lists:member(IndexEntry, BlackList) of
        true ->
            BlackList;
        false ->
            NewList = [IndexEntry | BlackList],
            config:set("dreyfus", "black_list", NewList),
            NewList
    end,
    UpdatedList;
add_bl_element(_IndexEntry) ->
    config:get("dreyfus", "black_list", []).



remove_bl_element(DbName, GroupId, IndexName) when is_list(DbName),
        is_list(GroupId), is_list(IndexName) ->
    remove_bl_element([DbName, GroupId, IndexName]);

remove_bl_element(DbName, GroupId, IndexName) when is_binary(DbName),
        is_binary(GroupId), is_binary(IndexName) ->
    remove_bl_element([?b2l(DbName), ?b2l(GroupId), ?b2l(IndexName)]).

remove_bl_element(IndexEntry) when is_list(IndexEntry) ->
    BlackList = config:get("dreyfus", "black_list", []),
    UpdatedList = case lists:member(IndexEntry, BlackList) of
        true ->
            NewList = lists:delete(IndexEntry, BlackList),
            config:set("dreyfus", "black_list", NewList),
            NewList;
        false ->
            BlackList
    end,
    UpdatedList;
remove_bl_element(_IndexEntry) ->
    config:get("dreyfus", "black_list", []).
