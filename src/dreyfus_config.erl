 -module(dreyfus_config).

 -export([data/0, get/1]).

data() ->
    Data = config:get("dreyfus", "black_list", []),
    [{K, true} || K <- Data].

get(Key) ->
    Handle = couch_epi:get_handle({dreyfus, black_list}),
    couch_epi:get_value(Handle, dreyfus, Key).
