-module(cets_dist_blocker_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/logger.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [{group, all}].

groups() ->
    [{all, [sequence, {repeat_until_any_fail, 2}], all_cases()}].

all_cases() ->
    [
        dist_blocker_waits_for_cleaning,
        dist_blocker_unblocks_if_cleaner_goes_down
    ].

init_per_suite(Config) ->
    Names = [peer_ct2],
    {Nodes, Peers} = lists:unzip([cets_test_peer:start_node(N) || N <- Names]),
    [
        {nodes, maps:from_list(lists:zip(Names, Nodes))},
        {peers, maps:from_list(lists:zip(Names, Peers))}
        | Config
    ].

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Name, Config) ->
    init_per_testcase_generic(Name, Config).

init_per_testcase_generic(Name, Config) ->
    [{testcase, Name} | Config].

end_per_testcase(_, _Config) ->
    ok.

dist_blocker_waits_for_cleaning(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    cets_dist_blocker:add_cleaner(self()),
    pong = net_adm:ping(Node2),
    true = erlang:disconnect_node(Node2),
    %% Connection is blocked
    pang = net_adm:ping(Node2),
    cets_dist_blocker:cleaning_done(self(), Node2),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

dist_blocker_unblocks_if_cleaner_goes_down(Config) ->
    Me = self(),
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    Cleaner = proc_lib:spawn(fun() ->
        cets_dist_blocker:add_cleaner(self()),
        Me ! added,
        timer:sleep(infinity)
    end),
    receive
        added -> ok
    after 5000 -> ct:fail(timeout)
    end,
    pong = net_adm:ping(Node2),
    true = erlang:disconnect_node(Node2),
    %% Connection is blocked
    pang = net_adm:ping(Node2),
    erlang:exit(Cleaner, killed),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).
