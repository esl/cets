-module(kiss_SUITE).
-include_lib("common_test/include/ct.hrl").
 
-compile([export_all]).
 
all() -> [ets_tests].
 
init_per_suite(Config) ->
    {ok, Node2} = ct_slave:start(ct2, [{monitor_master, true}]),
    [{node2, Node2}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(ets_tests, Config) ->
    Config.
 
end_per_testcase(ets_tests, Config) ->
    ok.
 
ets_tests(Config) ->
    Node2 = proplists:get_value(node2, Config),
    kiss:start(tab1, #{}),
    rpc:call(Node2, kiss, start, [tab1, #{}]),
    ok.
