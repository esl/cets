-module(cets_disco_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-compile([export_all, nowarn_export_all]).

-import(cets_test_setup, [
    start/2,
    start_local/1,
    start_local/2,
    start_disco/2,
    make_name/1,
    make_name/2,
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
    make_signalling_process/0
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
        {group, cets},
        {group, cets_seq},
        {group, cets_seq_no_log}
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
        disco_wait_for_get_nodes_works,
        disco_wait_for_get_nodes_blocks_and_returns,
        disco_wait_for_get_nodes_when_get_nodes_needs_to_be_retried,
        test_multinode_auto_discovery,
        test_disco_add_table,
        test_disco_delete_table,
        test_disco_delete_unknown_table,
        test_disco_delete_table_twice,
        test_disco_file_appears,
        test_disco_handles_bad_node,
        cets_discovery_fun_backend_works,
        test_disco_add_table_twice,
        test_disco_add_two_tables,
        disco_retried_if_get_nodes_fail,
        disco_uses_regular_retry_interval_in_the_regular_phase,
        disco_uses_regular_retry_interval_in_the_regular_phase_after_node_down,
        disco_uses_regular_retry_interval_in_the_regular_phase_after_expired_node_down,
        disco_handles_node_up_and_down,
        unexpected_nodedown_is_ignored_by_disco
    ].

seq_cases() ->
    [
        disco_logs_nodeup,
        disco_logs_nodedown,
        disco_logs_nodeup_after_downtime,
        disco_logs_node_reconnects_after_downtime,
        disco_node_up_timestamp_is_remembered,
        disco_node_down_timestamp_is_remembered,
        disco_nodeup_timestamp_is_updated_after_node_reconnects,
        disco_node_start_timestamp_is_updated_after_node_restarts,
        disco_late_pang_result_arrives_after_node_went_up,
        disco_nodeup_triggers_check_and_get_nodes,
        %% Cannot be run in parallel with other tests because checks all logging messages.
        logging_when_failing_join_with_disco,
        disco_connects_to_unconnected_node
    ].

cets_seq_no_log_cases() ->
    [
        disco_node_up_timestamp_is_remembered,
        disco_node_down_timestamp_is_remembered,
        disco_nodeup_timestamp_is_updated_after_node_reconnects,
        disco_node_start_timestamp_is_updated_after_node_restarts,
        disco_late_pang_result_arrives_after_node_went_up
    ].

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

init_per_testcase(test_multinode_auto_discovery = Name, Config) ->
    ct:make_priv_dir(),
    init_per_testcase_generic(Name, Config);
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

disco_wait_for_get_nodes_works(_Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    ok = cets_discovery:wait_for_get_nodes(Disco, 5000).

disco_wait_for_get_nodes_blocks_and_returns(Config) ->
    Tab = make_name(Config, 1),
    {ok, _Pid} = start_local(Tab, #{}),
    SignallingPid = make_signalling_process(),
    F = fun(State) ->
        wait_for_down(SignallingPid),
        {{ok, []}, State}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    cets_discovery:add_table(Disco, Tab),
    %% Enter into a blocking get_nodes function
    Disco ! check,
    %% Do it async, because it would block is
    WaitPid = spawn_link(fun() -> ok = cets_discovery:wait_for_get_nodes(Disco, 5000) end),
    Cond = fun() ->
        length(maps:get(pending_wait_for_get_nodes, cets_discovery:system_info(Disco)))
    end,
    cets_test_wait:wait_until(Cond, 1),
    %% Unblock get_nodes call
    SignallingPid ! stop,
    %% wait_for_get_nodes returns
    wait_for_down(WaitPid),
    ok.

%% Check that wait_for_get_nodes waits in case get_nodes should be retried
disco_wait_for_get_nodes_when_get_nodes_needs_to_be_retried(Config) ->
    Me = self(),
    Tab = make_name(Config, 1),
    {ok, _Pid} = start_local(Tab, #{}),
    SignallingPid1 = make_signalling_process(),
    SignallingPid2 = make_signalling_process(),
    F = fun
        (State = #{step := 1}) ->
            wait_for_down(SignallingPid1),
            {{ok, []}, State#{step => 2}};
        (State = #{step := 2}) ->
            Me ! entered_get_nodes2,
            wait_for_down(SignallingPid2),
            {{ok, []}, State#{step => 2}}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F, step => 1
    }),
    cets_discovery:add_table(Disco, Tab),
    %% Enter into a blocking get_nodes function
    Disco ! check,
    %% Do it async, because it would block is
    WaitPid = spawn_link(fun() -> ok = cets_discovery:wait_for_get_nodes(Disco, 5000) end),
    Cond = fun() ->
        length(maps:get(pending_wait_for_get_nodes, cets_discovery:system_info(Disco)))
    end,
    cets_test_wait:wait_until(Cond, 1),
    %% Set should_retry_get_nodes
    Disco ! check,
    %% Ensure check message is received
    cets_discovery:system_info(Disco),
    %% Unblock first get_nodes call
    SignallingPid1 ! stop,
    receive_message(entered_get_nodes2),
    %% Still waiting for get_nodes being retried
    true = erlang:is_process_alive(WaitPid),
    %% It returns finally after second get_nodes call
    SignallingPid2 ! stop,
    wait_for_down(WaitPid),
    ok.

test_multinode_auto_discovery(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco.txt"),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    {ok, Disco} = cets_discovery:start_link(#{tables => [Tab], disco_file => FileName}),
    %% Disco is async, so we have to wait for the final state
    ok = wait_for_ready(Disco, 5000),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco),
    #{verify_ready := []} =
        cets_discovery:system_info(Disco),
    ok.

test_disco_add_table(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco.txt"),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    {ok, Disco} = cets_discovery:start_link(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    %% Disco is async, so we have to wait for the final state
    ok = wait_for_ready(Disco, 5000),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco),
    ok.

test_disco_delete_table(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Tab = make_name(Config),
    cets_discovery:add_table(Disco, Tab),
    #{tables := [Tab]} = cets_discovery:system_info(Disco),
    cets_discovery:delete_table(Disco, Tab),
    #{tables := []} = cets_discovery:system_info(Disco).

test_disco_delete_unknown_table(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Tab = make_name(Config),
    cets_discovery:delete_table(Disco, Tab),
    #{tables := []} = cets_discovery:system_info(Disco).

test_disco_delete_table_twice(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Tab = make_name(Config),
    cets_discovery:add_table(Disco, Tab),
    #{tables := [Tab]} = cets_discovery:system_info(Disco),
    cets_discovery:delete_table(Disco, Tab),
    cets_discovery:delete_table(Disco, Tab),
    #{tables := []} = cets_discovery:system_info(Disco).

test_disco_file_appears(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco3.txt"),
    file:delete(FileName),
    {ok, Disco} = cets_discovery:start_link(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end,
        after_error
    ),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    %% Disco is async, so we have to wait for the final state
    ok = wait_for_ready(Disco, 5000),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco),
    ok.

test_disco_handles_bad_node(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco_badnode.txt"),
    ok = file:write_file(FileName, io_lib:format("badnode@localhost~n~s~n~s~n", [Node1, Node2])),
    {ok, Disco} = cets_discovery:start_link(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    %% Check that wait_for_ready would not block forever:
    ok = wait_for_ready(Disco, 5000),
    %% Check if the node sent pang:
    #{unavailable_nodes := ['badnode@localhost']} = cets_discovery:system_info(Disco),
    %% Check that other nodes are discovered fine
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco).

cets_discovery_fun_backend_works(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    F = fun(State) -> {{ok, [Node1, Node2]}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    cets_discovery:add_table(Disco, Tab),
    ok = wait_for_ready(Disco, 5000),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco).

test_disco_add_table_twice(Config) ->
    Dir = proplists:get_value(priv_dir, Config),
    FileName = filename:join(Dir, "disco.txt"),
    {ok, Disco} = cets_discovery:start_link(#{tables => [], disco_file => FileName}),
    Tab = make_name(Config),
    {ok, _Pid} = start_local(Tab),
    cets_discovery:add_table(Disco, Tab),
    cets_discovery:add_table(Disco, Tab),
    %% Check that everything is fine
    #{tables := [Tab]} = cets_discovery:system_info(Disco).

test_disco_add_two_tables(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, _} = start(Node1, Tab1),
    {ok, _} = start(Node2, Tab1),
    {ok, _} = start(Node1, Tab2),
    {ok, _} = start(Node2, Tab2),
    Me = self(),
    F = fun
        (State = #{waited := true}) ->
            Me ! called_after_waited,
            {{ok, [Node1, Node2]}, State};
        (State) ->
            wait_till_test_stage(Me, sent_both),
            Me ! waited_for_sent_both,
            {{ok, [Node1, Node2]}, State#{waited => true}}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    %% Add two tables async
    cets_discovery:add_table(Disco, Tab1),
    %% After the first table, Disco would get blocked in get_nodes function (see wait_till_test_stage in F above)
    cets_discovery:add_table(Disco, Tab2),
    put(test_stage, sent_both),
    %% Just ensure wait_till_test_stage function works:
    wait_till_test_stage(Me, sent_both),
    %% First check is done, the second check should be triggered asap
    %% (i.e. because of should_retry_get_nodes=true set in state)
    receive_message(waited_for_sent_both),
    %% try_joining would be called after set_nodes,
    %% but it is async, so wait until it is done:
    cets_test_wait:wait_until(
        fun() ->
            maps:with(
                [get_nodes_status, should_retry_get_nodes, join_status, should_retry_join],
                cets_discovery:system_info(Disco)
            )
        end,
        #{
            get_nodes_status => not_running,
            should_retry_get_nodes => false,
            join_status => not_running,
            should_retry_join => false
        }
    ),
    [
        #{memory := _, nodes := [Node1, Node2], size := 0, table := Tab1},
        #{memory := _, nodes := [Node1, Node2], size := 0, table := Tab2}
    ] =
        cets_discovery:info(Disco),
    ok.

disco_retried_if_get_nodes_fail(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    F = fun(State) ->
        {{error, simulate_error}, State}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end,
        after_error
    ),
    ok.

disco_uses_regular_retry_interval_in_the_regular_phase(Config) ->
    #{disco := Disco} = generic_disco_uses_regular_retry_interval_in_the_regular_phase(Config),
    #{phase := regular, retry_type := regular} = cets_discovery:system_info(Disco).

%% Similar to disco_uses_regular_retry_interval_in_the_regular_phase, but has nodedown
disco_uses_regular_retry_interval_in_the_regular_phase_after_node_down(Config) ->
    SysInfo = generic_disco_uses_regular_retry_interval_in_the_regular_phase(Config),
    #{disco := Disco, node2 := Node2} = SysInfo,
    Disco ! {nodedown, Node2},
    #{phase := regular, retry_type := after_nodedown} = cets_discovery:system_info(Disco).

%% Similar to disco_uses_regular_retry_interval_in_the_regular_phase_after_node_down, but we simulate long downtime
disco_uses_regular_retry_interval_in_the_regular_phase_after_expired_node_down(Config) ->
    #{disco := Disco, node2 := Node2} = generic_disco_uses_regular_retry_interval_in_the_regular_phase(
        Config
    ),
    Disco ! {nodedown, Node2},
    TestTimestamp = erlang:system_time(millisecond) - timer:seconds(1000),
    cets_test_helper:set_nodedown_timestamp(Disco, Node2, TestTimestamp),
    #{phase := regular, retry_type := regular} = cets_discovery:system_info(Disco).

generic_disco_uses_regular_retry_interval_in_the_regular_phase(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    F = fun(State) -> {{ok, [Node1, Node2]}, State} end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Disco ! enter_regular_phase,
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end, regular
    ),
    #{disco => Disco, node2 => Node2}.

disco_handles_node_up_and_down(Config) ->
    BadNode = 'badnode@localhost',
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    F = fun(State) ->
        {{ok, [Node1, Node2, BadNode]}, State}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    cets_discovery:add_table(Disco, Tab),
    %% get_nodes call is async, so wait for it
    cets_test_wait:wait_until(
        fun() -> length(maps:get(nodes, cets_discovery:system_info(Disco))) end,
        3
    ),
    Disco ! {nodeup, BadNode},
    Disco ! {nodedown, BadNode},
    %% Check that wait_for_ready still works
    ok = wait_for_ready(Disco, 5000).

unexpected_nodedown_is_ignored_by_disco(Config) ->
    %% Theoretically, should not happen
    %% Still, check that we do not crash in this case
    DiscoName = disco_name(Config),
    F = fun(State) -> {{ok, []}, State} end,
    Disco = start_disco(node(), #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    #{start_time := StartTime} = cets_discovery:system_info(Disco),
    Disco ! {nodedown, 'cets@badnode'},
    %% Check that we are still running
    #{start_time := StartTime} = cets_discovery:system_info(Disco),
    ok.

disco_logs_nodeup(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    #{disco := Disco, node2 := Node2} = setup_two_nodes_and_discovery(Config),
    %% There could be several disco processes still running from the previous tests,
    %% filter out logs by pid.
    receive
        {log, ?FUNCTION_NAME, #{
            level := warning,
            meta := #{pid := Disco},
            msg := {report, #{what := nodeup, remote_node := Node2} = R}
        }} = M ->
            ?assert(is_integer(maps:get(connected_nodes, R)), M),
            ?assert(is_integer(maps:get(time_since_startup_in_milliseconds, R)), M)
    after 5000 ->
        ct:fail(timeout)
    end.

disco_node_up_timestamp_is_remembered(Config) ->
    #{disco := Disco, node2 := Node2} = setup_two_nodes_and_discovery(Config),
    %% Check that nodeup is remembered
    wait_for_disco_timestamp_to_appear(Disco, nodeup_timestamps, Node2).

disco_logs_nodedown(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    ok = net_kernel:monitor_nodes(true),
    #{disco := Disco, node2 := Node2} = setup_two_nodes_and_discovery(Config, [wait, netsplit]),
    receive_message({nodedown, Node2}),
    receive
        {log, ?FUNCTION_NAME, #{
            level := warning,
            meta := #{pid := Disco},
            msg := {report, #{what := nodedown, remote_node := Node2} = R}
        }} = M ->
            ?assert(is_integer(maps:get(connected_nodes, R)), M),
            ?assert(is_integer(maps:get(time_since_startup_in_milliseconds, R)), M),
            ?assert(is_integer(maps:get(connected_millisecond_duration, R)), M)
    after 5000 ->
        ct:fail(timeout)
    end.

disco_node_down_timestamp_is_remembered(Config) ->
    #{disco := Disco, node2 := Node2} = setup_two_nodes_and_discovery(Config, [wait, netsplit]),
    %% Check that nodedown is remembered
    wait_for_disco_timestamp_to_appear(Disco, nodedown_timestamps, Node2).

disco_logs_nodeup_after_downtime(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    #{disco := Disco, node2 := Node2} = setup_two_nodes_and_discovery(Config, [wait, netsplit]),
    %% At this point cets_disco should reconnect nodes back automatically
    %% after retry_type_to_timeout(after_nodedown) time.
    %% We want to speed this up for tests though.
    Disco ! check,
    %% Receive a nodeup after the disconnect.
    %% This nodeup should contain the downtime_millisecond_duration field
    %% (initial nodeup should not contain this field).
    receive
        {log, ?FUNCTION_NAME, #{
            level := warning,
            meta := #{pid := Disco},
            msg :=
                {report,
                    #{
                        what := nodeup,
                        remote_node := Node2,
                        downtime_millisecond_duration := Downtime
                    } = R}
        }} = M ->
            ?assert(is_integer(maps:get(connected_nodes, R)), M),
            ?assert(is_integer(Downtime), M)
    after 5000 ->
        ct:fail(timeout)
    end.

disco_logs_node_reconnects_after_downtime(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Setup = setup_two_nodes_and_discovery(Config, [wait, disco2]),
    #{disco := Disco, node1 := Node1, node2 := Node2, peer2 := Peer2} = Setup,
    %% Check that a start timestamp from a remote node is stored
    Info = cets_discovery:system_info(Disco),
    ?assertMatch(#{node_start_timestamps := #{Node2 := _}}, Info),
    disconnect_node(Peer2, Node1),
    receive
        {log, ?FUNCTION_NAME, #{
            level := warning,
            meta := #{pid := Disco},
            msg :=
                {report, #{
                    what := node_reconnects,
                    start_time := StartTime,
                    remote_node := Node2
                }}
        }} = M ->
            ?assert(is_integer(StartTime), M)
    after 5000 ->
        ct:fail(timeout)
    end.

disco_nodeup_timestamp_is_updated_after_node_reconnects(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Setup = setup_two_nodes_and_discovery(Config, [wait, disco2]),
    #{disco := Disco, node2 := Node2} = Setup,
    OldTimestamp = cets_test_helper:get_disco_timestamp(Disco, nodeup_timestamps, Node2),
    disconnect_node_by_name(Config, ct2),
    wait_for_disco_timestamp_to_be_updated(Disco, nodeup_timestamps, Node2, OldTimestamp).

disco_node_start_timestamp_is_updated_after_node_restarts(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Setup = setup_two_nodes_and_discovery(Config, [wait, disco2]),
    #{disco := Disco, node2 := Node2} = Setup,
    OldTimestamp = cets_test_helper:get_disco_timestamp(Disco, node_start_timestamps, Node2),
    simulate_disco_restart(Setup),
    wait_for_disco_timestamp_to_be_updated(Disco, node_start_timestamps, Node2, OldTimestamp).

disco_late_pang_result_arrives_after_node_went_up(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    %% unavailable_nodes list contains nodes which have not responded to pings.
    %% Ping is async though.
    %% So, there could be the situation when the result of ping would be processed
    %% after the node actually got connected.
    meck:new(cets_ping, [passthrough]),
    Me = self(),
    meck:expect(cets_ping, send_ping_result, fun(Pid, Node, _PingResult) ->
        %% Wait until Node is up
        Cond = fun() -> lists:member(Node, nodes()) end,
        cets_test_wait:wait_until(Cond, true),
        Me ! send_ping_result_called,
        %% Return pang to cets_discovery.
        %% cets_join does not use send_ping_result function
        %% and would receive pong and join correctly.
        meck:passthrough([Pid, Node, pang])
    end),
    try
        %% setup_two_nodes_and_discovery would call disconnect_node/2 function
        Setup = setup_two_nodes_and_discovery(Config, [wait, disco2]),
        receive_message(send_ping_result_called),
        #{disco_name := DiscoName} = Setup,
        Status = cets_status:status(DiscoName),
        %% Check that pang is ignored and unavailable_nodes list is empty.
        ?assertMatch([], maps:get(unavailable_nodes, Status)),
        ?assertMatch([Node1, Node2], maps:get(joined_nodes, Status))
    after
        meck:unload()
    end.

disco_nodeup_triggers_check_and_get_nodes(Config) ->
    Setup = setup_two_nodes_and_discovery(Config, [wait, notify_get_nodes]),
    #{disco := Disco, node2 := Node2} = Setup,
    flush_message(get_nodes),
    Disco ! {nodeup, Node2},
    receive_message(get_nodes).

disco_connects_to_unconnected_node(Config) ->
    Node1 = node(),
    #{ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    disconnect_node(Peer5, Node1),
    cets_test_wait:wait_until(
        fun() -> lists:member(node(), rpc(Peer5, erlang, nodes, [])) end, false
    ),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Peer5, Tab),
    F = fun(State) ->
        {{ok, [Node1, Node5]}, State}
    end,
    {ok, Disco} = cets_discovery:start_link(#{
        backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    cets_discovery:add_table(Disco, Tab),
    ok = wait_for_ready(Disco, 5000).

logging_when_failing_join_with_disco(Config) ->
    %% Simulate cets:other_pids/1 failing with reason:
    %%  {{nodedown,'mongooseim@mongooseim-1.mongooseim.default.svc.cluster.local'},
    %%   {gen_server,call,[<30887.438.0>,other_servers,infinity]}}
    %% We use peer module to still have a connection after a disconnect from the remote node.
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Node1 = node(),
    #{ct2 := Peer2} = proplists:get_value(peers, Config),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Peer2, Tab),
    meck:new(cets, [passthrough]),
    meck:expect(cets, other_pids, fun
        (Server) when Server =:= Pid2 ->
            block_node(Node2, Peer2),
            wait_for_down(Pid2),
            meck:passthrough([Server]);
        (Server) ->
            meck:passthrough([Server])
    end),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    Disco = start_disco(Node1, #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    try
        cets_discovery:add_table(Disco, Tab),
        timer:sleep(100),
        Logs = cets_test_log:receive_all_logs(?FUNCTION_NAME),
        Reason = {{nodedown, Node2}, {gen_server, call, [Pid2, other_servers, infinity]}},
        MatchedLogs = [
            Log
         || #{
                level := error,
                msg :=
                    {report, #{
                        what := task_failed,
                        reason := Reason2
                    }}
            } = Log <- Logs,
            Reason =:= Reason2
        ],
        %% Only one message is logged
        ?assertMatch([_], MatchedLogs, Logs)
    after
        meck:unload(),
        reconnect_node(Node2, Peer2),
        cets:stop(Pid2)
    end,
    ok.
