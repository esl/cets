-module(cets_disco_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-compile([export_all, nowarn_export_all]).

-import(cets_test_setup, [
    setup_two_nodes_and_discovery/1,
    setup_two_nodes_and_discovery/2,
    simulate_disco_restart/1
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
    disconnect_node/2,
    disconnect_node_by_name/2
]).

-import(cets_test_helper, [assert_unique/1]).

all() ->
    [
        {group, cets_seq},
        {group, cets_seq_no_log}
    ].

groups() ->
    %% Cases should have unique names, because we name CETS servers based on case names
    [
        %% These tests actually simulate a netsplit on the distribution level.
        %% Though, global's prevent_overlapping_partitions option starts kicking
        %% all nodes from the cluster, so we have to be careful not to break other cases.
        %% Setting prevent_overlapping_partitions=false on ct5 helps.
        {cets_seq, [sequence, {repeat_until_any_fail, 2}], assert_unique(seq_cases())},
        {cets_seq_no_log, [sequence, {repeat_until_any_fail, 2}],
            assert_unique(cets_seq_no_log_cases())}
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
        disco_nodeup_triggers_check_and_get_nodes
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
    cets_test_peer:start([ct2], Config).

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
