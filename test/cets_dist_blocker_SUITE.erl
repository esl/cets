-module(cets_dist_blocker_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/logger.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [
        {group, blocker},
        {group, unknown}
    ].

groups() ->
    [
        {blocker, [sequence, {repeat_until_any_fail, 2}], blocker_cases()},
        {unknown, [sequence, {repeat_until_any_fail, 2}], unknown_cases()}
    ].

blocker_cases() ->
    [
        waits_for_cleaning,
        unblocks_if_cleaner_goes_down,
        unblocks_if_cleaner_goes_down_and_second_cleaner_says_done,
        unblocks_if_cleaner_says_done_and_second_cleaner_goes_down,
        blocks_if_cleaner_says_done_and_second_cleaner_does_not_ack,
        skip_blocking_if_no_cleaners
    ].

unknown_cases() ->
    [
        unknown_down_message_is_ignored,
        unknown_message_is_ignored,
        unknown_cast_message_is_ignored,
        unknown_call_returns_error,
        code_change_returns_ok
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

%% Test blocking functionality

waits_for_cleaning(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    cets_dist_blocker:add_cleaner(self()),
    connect_and_disconnect(Node2),
    cets_dist_blocker:cleaning_done(self(), Node2),
    sync_blocker(Blocker),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

unblocks_if_cleaner_goes_down(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    Cleaner = spawn_cleaner(),
    connect_and_disconnect(Node2),
    erlang:exit(Cleaner, killed),
    sync_blocker(Blocker),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

unblocks_if_cleaner_goes_down_and_second_cleaner_says_done(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    %% Two cleaners
    cets_dist_blocker:add_cleaner(self()),
    Cleaner = spawn_cleaner(),
    connect_and_disconnect(Node2),
    erlang:exit(Cleaner, killed),
    wait_for_waiting_count(Blocker, 1),
    cets_dist_blocker:cleaning_done(self(), Node2),
    sync_blocker(Blocker),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

unblocks_if_cleaner_says_done_and_second_cleaner_goes_down(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    %% Two cleaners
    cets_dist_blocker:add_cleaner(self()),
    Cleaner = spawn_cleaner(),
    connect_and_disconnect(Node2),
    %% Different order comparing to unblocks_if_cleaner_goes_down_and_second_cleaner_says_done
    cets_dist_blocker:cleaning_done(self(), Node2),
    wait_for_waiting_count(Blocker, 1),
    erlang:exit(Cleaner, killed),
    sync_blocker(Blocker),
    %% Connection is unblocked
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

blocks_if_cleaner_says_done_and_second_cleaner_does_not_ack(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    %% Two cleaners
    cets_dist_blocker:add_cleaner(self()),
    _Cleaner = spawn_cleaner(),
    connect_and_disconnect(Node2),
    cets_dist_blocker:cleaning_done(self(), Node2),
    wait_for_waiting_count(Blocker, 1),
    sync_blocker(Blocker),
    %% Connection is still blocked
    pang = net_adm:ping(Node2),
    gen_server:stop(Blocker).

skip_blocking_if_no_cleaners(Config) ->
    #{peer_ct2 := Node2} = proplists:get_value(nodes, Config),
    {ok, Blocker} = cets_dist_blocker:start_link(),
    pong = net_adm:ping(Node2),
    true = erlang:disconnect_node(Node2),
    pong = net_adm:ping(Node2),
    gen_server:stop(Blocker).

%% Cover unknown message handling / code_change

unknown_down_message_is_ignored(_Config) ->
    {ok, Pid} = cets_dist_blocker:start_link(),
    RandPid = proc_lib:spawn(fun() -> ok end),
    Pid ! {'DOWN', make_ref(), process, RandPid, oops},
    still_works(Pid).

unknown_message_is_ignored(_Config) ->
    {ok, Pid} = cets_dist_blocker:start_link(),
    Pid ! oops,
    still_works(Pid).

unknown_cast_message_is_ignored(_Config) ->
    {ok, Pid} = cets_dist_blocker:start_link(),
    gen_server:cast(Pid, oops),
    still_works(Pid).

unknown_call_returns_error(_Config) ->
    {ok, Pid} = cets_dist_blocker:start_link(),
    {error, unexpected_call} = gen_server:call(Pid, oops),
    still_works(Pid).

code_change_returns_ok(_Config) ->
    {ok, Pid} = cets_dist_blocker:start_link(),
    sys:suspend(Pid),
    ok = sys:change_code(Pid, cets_dist_blocker, v2, []),
    sys:resume(Pid),
    still_works(Pid).

%% Helpers

still_works(Pid) ->
    #{} = sys:get_state(Pid).

spawn_cleaner() ->
    Me = self(),
    Cleaner = proc_lib:spawn(fun() ->
        cets_dist_blocker:add_cleaner(self()),
        Me ! added,
        timer:sleep(infinity)
    end),
    receive
        added -> ok
    after 5000 -> ct:fail(timeout)
    end,
    Cleaner.

%% Wait for the blocker to process pending nodeup/nodedown messages
sync_blocker(Blocker) ->
    sys:get_state(Blocker),
    ok.

wait_for_waiting_count(Blocker, Count) ->
    F = fun() -> length(maps:get(waiting, sys:get_state(Blocker))) end,
    cets_test_wait:wait_until(F, Count).

connect_and_disconnect(Node2) ->
    pong = net_adm:ping(Node2),
    true = erlang:disconnect_node(Node2),
    %% Connection is blocked
    pang = net_adm:ping(Node2),
    ok.
