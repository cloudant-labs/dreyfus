-module(dreyfus_config).


-export([
    init/0,
    reconfigure/0,
    get/1
]).


-define(MOD_NAME, dreyfus_config_dyn).
-define(ERL_FILE, "dreyfus_config_dyn.erl").


-spec init() -> ok.
init() ->
    reconfigure().


-spec reconfigure() -> ok.
reconfigure() ->
    {ok, ?MOD_NAME, Bin} = compile:forms(forms(), [verbose, report_errors]),
    code:purge(?MOD_NAME),
    {module, ?MOD_NAME} = code:load_binary(?MOD_NAME, ?ERL_FILE, Bin),
    ok.


-spec get(atom()) -> term().
get(Key) ->
    ?MOD_NAME:get(Key).


-spec entries() -> [string()].
entries() ->
    [
        {black_list, "black_list", []}
    ].


-spec forms() -> [erl_syntax:syntaxTree()].
forms() ->
    GetFunClauses = lists:map(fun({FunKey, CfgKey, Default}) ->
        FunVal = config:get("dreyfus", CfgKey, Default),
        Patterns = [erl_syntax:abstract(FunKey)],
        Bodies = [erl_syntax:abstract(FunVal)],
        erl_syntax:clause(Patterns, none, Bodies)
    end, entries()),

    Statements = [
        % -module(?MOD_NAME)
        erl_syntax:attribute(
            erl_syntax:atom(module),
            [erl_syntax:atom(?MOD_NAME)]
        ),

        % -export([get/1]).
        erl_syntax:attribute(
            erl_syntax:atom(export),
            [erl_syntax:list([
                erl_syntax:arity_qualifier(
                    erl_syntax:atom(get),
                    erl_syntax:integer(1))
            ])]
        ),

        % list(Key) -> Value.
        erl_syntax:function(erl_syntax:atom(get), GetFunClauses)
    ],
    [erl_syntax:revert(X) || X <- Statements].
