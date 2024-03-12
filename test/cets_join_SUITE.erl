-module(cets_join_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-compile([export_all, nowarn_export_all]).

-import(cets_test_setup, [
    start/2,
    start_local/1,
    start_local/2,
    start_disco/2,
    start_simple_disco/0,
    make_name/1,
    make_name/2,
    lock_name/1,
    disco_name/1
]).

-import(cets_test_wait, [
    wait_for_down/1,
    wait_for_ready/2,
    wait_till_test_stage/2
]).

-import(cets_test_setup, [
    setup_two_nodes_and_discovery/1,
    setup_two_nodes_and_discovery/2,
    simulate_disco_restart/1,
    make_signalling_process/0,
    given_two_joined_tables/1,
    given_two_joined_tables/2,
    given_3_servers/2
]).

-import(cets_test_wait, [
    wait_for_disco_timestamp_to_appear/3,
    wait_for_disco_timestamp_to_be_updated/4
]).

-import(cets_test_receive, [
    receive_message/1,
    flush_message/1
]).

-import(cets_test_peer, [
    block_node/2,
    reconnect_node/2,
    disconnect_node/2,
    disconnect_node_by_name/2
]).

-import(cets_test_rpc, [
    rpc/4
]).

-import(cets_test_helper, [assert_unique/1]).

-import(cets_test_rpc, [
    other_nodes/2
]).

all() ->
    [
        {group, cets}
        %       {group, cets_seq},
        %       {group, cets_seq_no_log}
    ].

groups() ->
    %% Cases should have unique names, because we name CETS servers based on case names
    [
        {cets, [parallel, {repeat_until_any_fail, 3}], assert_unique(cases())},
        %% These tests actually simulate a netsplit on the distribution level.
        %% Though, global's prevent_overlapping_partitions option starts kicking
        %% all nodes from the cluster, so we have to be careful not to break other cases.
        %% Setting prevent_overlapping_partitions=false on ct5 helps.
        {cets_seq, [sequence, {repeat_until_any_fail, 2}], assert_unique(seq_cases())},
        {cets_seq_no_log, [sequence, {repeat_until_any_fail, 2}],
            assert_unique(cets_seq_no_log_cases())}
    ].

cases() ->
    [
        join_works,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_more_keys,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_keypos2,
        bag_with_conflict_handler_not_allowed,
        join_with_the_same_pid,
        join_ref_is_same_after_join,
        join_fails_because_server_process_not_found,
        join_fails_because_server_process_not_found_before_get_pids,
        join_fails_before_send_dump,
        join_fails_before_send_dump_and_there_are_pending_remote_ops
    ].

seq_cases() ->
    [].

cets_seq_no_log_cases() ->
    [].

init_per_suite(Config) ->
    cets_test_setup:init_cleanup_table(),
    cets_test_peer:start([ct2, ct5], Config).

end_per_suite(Config) ->
    cets_test_setup:remove_cleanup_table(),
    cets_test_peer:stop(Config),
    Config.

init_per_group(Group, Config) when Group == cets_seq_no_log; Group == cets_no_log ->
    [ok = logger:set_module_level(M, none) || M <- log_modules()],
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group == cets_seq_no_log; Group == cets_no_log ->
    [ok = logger:unset_module_level(M) || M <- log_modules()],
    Config;
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Name, Config) ->
    init_per_testcase_generic(Name, Config).

init_per_testcase_generic(Name, Config) ->
    [{testcase, Name} | Config].

end_per_testcase(_, _Config) ->
    cets_test_setup:wait_for_cleanup(),
    ok.

%% Modules that use a multiline LOG_ macro
log_modules() ->
    [cets, cets_call, cets_long, cets_join, cets_discovery].

join_works(Config) ->
    given_two_joined_tables(Config).

join_works_with_existing_data(Config) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1),
    {ok, Pid2} = start_local(Tab2),
    cets:insert(Tab1, {alice, 32}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    [{alice, 32}] = ets:lookup(Tab2, alice).

%% This testcase tests an edgecase: inserting with the same key from two nodes.
%% Usually, inserting with the same key from two different nodes is not possible
%% (because the node-name is a part of the key).
join_works_with_existing_data_with_conflicts(Config) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1),
    {ok, Pid2} = start_local(Tab2),
    cets:insert(Tab1, {alice, 32}),
    cets:insert(Tab2, {alice, 33}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    %% We insert data from other table into our table when merging, so the values get swapped
    [{alice, 33}] = ets:lookup(Tab1, alice),
    [{alice, 32}] = ets:lookup(Tab2, alice).

join_works_with_existing_data_with_conflicts_and_defined_conflict_handler(Config) ->
    Opts = #{handle_conflict => fun resolve_highest/2},
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1, Opts),
    {ok, Pid2} = start_local(Tab2, Opts),
    cets:insert(Tab1, {alice, 32}),
    cets:insert(Tab2, {alice, 33}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    %% Key with the highest Number remains
    [{alice, 33}] = ets:lookup(Tab1, alice),
    [{alice, 33}] = ets:lookup(Tab2, alice).

join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_more_keys(Config) ->
    %% Deeper testing of cets_join:apply_resolver function
    Opts = #{handle_conflict => fun resolve_highest/2},
    #{tabs := [T1, T2, T3], pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config, Opts),
    cets:insert_many(T1, [{alice, 32}, {bob, 10}, {michal, 40}]),
    cets:insert_many(T2, [{alice, 33}, {kate, 3}, {michal, 2}]),
    %% Join will copy and merge existing tables
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid3),
    %% Key with the highest Number remains
    Dump = [{alice, 33}, {bob, 10}, {kate, 3}, {michal, 40}],
    Dump = cets:dump(T1),
    Dump = cets:dump(T2),
    Dump = cets:dump(T3).

-record(user, {name, age, updated}).

%% Test with records (which require keypos = 2 option)
join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_keypos2(Config) ->
    Opts = #{handle_conflict => fun resolve_user_conflict/2, keypos => 2},
    T1 = make_name(Config, 1),
    T2 = make_name(Config, 2),
    {ok, Pid1} = start_local(T1, Opts),
    {ok, Pid2} = start_local(T2, Opts),
    cets:insert(T1, #user{name = alice, age = 30, updated = erlang:system_time()}),
    cets:insert(T2, #user{name = alice, age = 25, updated = erlang:system_time()}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(keypos2_lock, #{}, Pid1, Pid2),
    %% Last inserted record is in the table
    [#user{age = 25}] = ets:lookup(T1, alice),
    [#user{age = 25}] = ets:lookup(T2, alice).

%% Keep record with highest timestamp
resolve_user_conflict(U1 = #user{updated = TS1}, _U2 = #user{updated = TS2}) when
    TS1 > TS2
->
    U1;
resolve_user_conflict(_U1, U2) ->
    U2.

resolve_highest({K, A}, {K, B}) ->
    {K, max(A, B)}.

bag_with_conflict_handler_not_allowed(Config) ->
    {error, [bag_with_conflict_handler]} =
        cets:start(make_name(Config), #{handle_conflict => fun resolve_highest/2, type => bag}).

join_with_the_same_pid(Config) ->
    Tab = make_name(Config),
    {ok, Pid} = start_local(Tab),
    %% Just insert something into a table to check later the size
    cets:insert(Tab, {1, 1}),
    link(Pid),
    {error, join_with_the_same_pid} = cets_join:join(lock_name(Config), #{}, Pid, Pid),
    Nodes = [node()],
    %% The process is still running and no data loss (i.e. size is not zero)
    #{nodes := Nodes, size := 1} = cets:info(Pid).

join_ref_is_same_after_join(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    #{join_ref := JoinRef} = cets:info(Pid1),
    #{join_ref := JoinRef} = cets:info(Pid2).

join_fails_because_server_process_not_found(Config) ->
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    F = fun
        (join_start) ->
            exit(Pid1, sim_error);
        (_) ->
            ok
    end,
    {error, {task_failed, {noproc, {gen_server, call, [Pid1, get_info, infinity]}}, _}} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}).

join_fails_because_server_process_not_found_before_get_pids(Config) ->
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    F = fun
        (before_get_pids) ->
            exit(Pid1, sim_error);
        (_) ->
            ok
    end,
    {error, {task_failed, {noproc, {gen_server, call, [Pid1, other_servers, infinity]}}, _}} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}).

join_fails_before_send_dump(Config) ->
    Me = self(),
    DownFn = fun(#{remote_pid := RemotePid, table := _Tab}) ->
        Me ! {down_called, self(), RemotePid}
    end,
    {ok, Pid1} = start_local(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = start_local(make_name(Config, 2), #{}),
    cets:insert(Pid1, {1}),
    cets:insert(Pid2, {2}),
    F = fun
        ({before_send_dump, P}) when Pid1 =:= P ->
            Me ! before_send_dump_called_for_pid1;
        ({before_send_dump, P}) when Pid2 =:= P ->
            error(sim_error);
        (_) ->
            ok
    end,
    ?assertMatch(
        {error, {task_failed, sim_error, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F})
    ),
    %% Ensure we sent dump to Pid1
    receive_message(before_send_dump_called_for_pid1),
    %% Not joined, some data exchanged
    cets:ping_all(Pid1),
    cets:ping_all(Pid2),
    [] = cets:other_pids(Pid1),
    [] = cets:other_pids(Pid2),
    %% Pid1 applied new version of dump
    %% Though, it got disconnected after
    {ok, [{1}, {2}]} = cets:remote_dump(Pid1),
    %% Pid2 rejected changes
    {ok, [{2}]} = cets:remote_dump(Pid2),
    receive_message({down_called, Pid1, Pid2}).

%% Checks that remote ops are dropped if join_ref does not match in the state and in remote_op message
join_fails_before_send_dump_and_there_are_pending_remote_ops(Config) ->
    Me = self(),
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    F = fun
        ({before_send_dump, P}) when Pid1 =:= P ->
            Me ! before_send_dump_called_for_pid1;
        ({before_send_dump, P}) when Pid2 =:= P ->
            sys:suspend(Pid2),
            error(sim_error);
        (before_unpause) ->
            %% Crash in before_unpause, otherwise cets_join will block in cets:unpause/2
            %% (because Pid2 is suspended).
            %% Servers would be unpaused automatically though, because cets_join process exits
            %% (i.e. cets:unpause/2 call is totally optional)
            error(sim_error2);
        (_) ->
            ok
    end,
    ?assertMatch(
        {error, {task_failed, sim_error2, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F})
    ),
    %% Ensure we sent dump to Pid1
    receive_message(before_send_dump_called_for_pid1),
    cets:insert_request(Pid1, {1}),
    %% Check that the remote_op has reached Pid2 message box
    cets_test_wait:wait_for_remote_ops_in_the_message_box(Pid2, 1),
    sys:resume(Pid2),
    %% Wait till remote_op is processed
    cets:ping(Pid2),
    %% Check that the insert was ignored
    {ok, []} = cets:remote_dump(Pid2).
