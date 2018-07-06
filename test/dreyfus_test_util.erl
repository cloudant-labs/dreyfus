-module(dreyfus_test_util).

-compile(export_all).


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
    after 1000 ->
        erlang:error(config_change_timeout)
    end.


config_files() ->
    Path = filename:dirname(code:which(?MODULE)),
    Name = filename:join(Path, "dreyfus_test.ini"),
    ok = file:write_file(Name, "[log]\nwriter = ets\n"),
    [Name].


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