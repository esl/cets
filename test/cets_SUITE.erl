%% Code conventions:
%% - Use PeerNum for peer pids (i.e. Peer2, Peer3...)
%% - Use NodeNum for nodes (i.e. Node2, Node3...)
%% - Node1 is the test node
%% - Use assertException macro to test errors
%% - Tests should cleanup after themself (we use repeat_until_any_fail to ensure this)
%% - Use repeat_until_any_fail=100 to ensure new tests are not flaky
-module(cets_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
        {group, cets},
        %% To improve the code coverage we need to test with logging disabled
        %% More info: https://github.com/erlang/otp/issues/7531
        {group, cets_no_log},
        {group, cets_seq},
        {group, cets_seq_no_log}
    ].

groups() ->
    [
        {cets, [parallel, {repeat_until_any_fail, 3}], cases() ++ only_for_logger_cases()},
        {cets_no_log, [parallel], cases()},
        %% These tests actually simulate a netsplit on the distribution level.
        %% Though, global's prevent_overlapping_partitions option starts kicking
        %% all nodes from the cluster, so we have to be careful not to break other cases.
        %% Setting prevent_overlapping_partitions=false on ct5 helps.
        {cets_seq, [sequence, {repeat_until_any_fail, 2}], seq_cases()},
        {cets_seq_no_log, [sequence, {repeat_until_any_fail, 2}], cets_seq_no_log_cases()}
    ].

cases() ->
    [
        inserted_records_could_be_read_back,
        insert_many_with_one_record,
        insert_many_with_two_records,
        delete_works,
        delete_many_works,
        join_works,
        inserted_records_could_be_read_back_from_replicated_table,
        join_works_with_existing_data,
        join_works_with_existing_data_with_conflicts,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_more_keys,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_keypos2,
        bag_with_conflict_handler_not_allowed,
        insert_new_works,
        insert_new_works_with_table_name,
        insert_new_works_when_leader_is_back,
        insert_new_when_new_leader_has_joined,
        insert_new_when_new_leader_has_joined_duplicate,
        insert_new_when_inconsistent_minimal,
        insert_new_when_inconsistent,
        insert_new_is_retried_when_leader_is_reelected,
        insert_new_fails_if_the_leader_dies,
        insert_new_fails_if_the_local_server_is_dead,
        insert_new_or_lookup_works,
        insert_serial_works,
        insert_serial_overwrites_data,
        insert_overwrites_data_inconsistently,
        insert_new_does_not_overwrite_data,
        insert_serial_overwrites_data_consistently,
        insert_serial_works_when_leader_is_back,
        insert_serial_blocks_when_leader_is_not_back,
        leader_is_the_same_in_metadata_after_join,
        join_with_the_same_pid,
        join_ref_is_same_after_join,
        join_fails_because_server_process_not_found,
        join_fails_because_server_process_not_found_before_get_pids,
        join_fails_before_send_dump,
        join_fails_before_send_dump_and_there_are_pending_remote_ops,
        send_dump_fails_during_join_because_receiver_exits,
        join_fails_in_check_fully_connected,
        join_fails_because_join_refs_do_not_match_for_nodes_in_segment,
        join_fails_because_pids_do_not_match_for_nodes_in_segment,
        join_fails_because_servers_overlap,
        remote_ops_are_ignored_if_join_ref_does_not_match,
        join_retried_if_lock_is_busy,
        send_dump_contains_already_added_servers,
        test_multinode,
        test_multinode_remote_insert,
        node_list_is_correct,
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
        disco_handles_node_up_and_down,
        status_available_nodes,
        status_available_nodes_do_not_contain_nodes_with_stopped_disco,
        status_unavailable_nodes,
        status_unavailable_nodes_is_subset_of_discovery_nodes,
        status_joined_nodes,
        status_discovery_works,
        status_discovered_nodes,
        status_remote_nodes_without_disco,
        status_remote_nodes_with_unknown_tables,
        status_remote_nodes_with_missing_nodes,
        status_conflict_nodes,
        disco_wait_for_get_nodes_works,
        disco_wait_for_get_nodes_blocks_and_returns,
        disco_wait_for_get_nodes_when_get_nodes_needs_to_be_retried,
        get_nodes_request,
        test_locally,
        handle_down_is_called,
        events_are_applied_in_the_correct_order_after_unpause,
        pause_multiple_times,
        unpause_twice,
        unpause_if_pause_owner_crashes,
        write_returns_if_remote_server_crashes,
        ack_process_stops_correctly,
        ack_process_handles_unknown_remote_server,
        ack_process_handles_unknown_from,
        ack_calling_add_when_server_list_is_empty_is_not_allowed,
        ping_all_using_name_works,
        insert_many_request,
        insert_many_requests,
        insert_many_requests_timeouts,
        insert_into_bag,
        delete_from_bag,
        delete_many_from_bag,
        delete_request_from_bag,
        delete_request_many_from_bag,
        insert_into_bag_is_replicated,
        insert_into_keypos_table,
        table_name_works,
        info_contains_opts,
        check_could_reach_each_other_fails,
        unknown_down_message_is_ignored,
        unknown_message_is_ignored,
        unknown_cast_message_is_ignored,
        unknown_message_is_ignored_in_ack_process,
        unknown_cast_message_is_ignored_in_ack_process,
        unknown_call_returns_error_from_ack_process,
        code_change_returns_ok,
        code_change_returns_ok_for_ack,
        run_spawn_forwards_errors,
        run_tracked_failed,
        run_tracked_logged,
        long_call_to_unknown_name_throws_pid_not_found,
        send_leader_op_throws_noproc,
        pinfo_returns_value,
        pinfo_returns_undefined,
        format_data_does_not_return_table_duplicates,
        cets_ping_non_existing_node,
        cets_ping_net_family,
        unexpected_nodedown_is_ignored_by_disco
    ].

only_for_logger_cases() ->
    [
        run_tracked_logged_check_logger,
        long_call_fails_because_linked_process_dies,
        logs_are_printed_when_join_fails_because_servers_overlap,
        join_done_already_while_waiting_for_lock_so_do_nothing,
        atom_error_is_logged_in_tracked,
        shutdown_reason_is_not_logged_in_tracked,
        other_reason_is_logged_in_tracked,
        nested_calls_errors_are_logged_once_with_tuple_reason,
        nested_calls_errors_are_logged_once_with_map_reason
    ].

seq_cases() ->
    [
        insert_returns_when_netsplit,
        inserts_after_netsplit_reconnects,
        disco_connects_to_unconnected_node,
        joining_not_fully_connected_node_is_not_allowed,
        joining_not_fully_connected_node_is_not_allowed2,
        %% Cannot be run in parallel with other tests because checks all logging messages.
        logging_when_failing_join_with_disco,
        cets_ping_all_returns_when_ping_crashes,
        join_interrupted_when_ping_crashes,
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
        ping_pairs_returns_pongs,
        ping_pairs_returns_earlier,
        pre_connect_fails_on_our_node,
        pre_connect_fails_on_one_of_the_nodes
    ].

cets_seq_no_log_cases() ->
    [
        join_interrupted_when_ping_crashes,
        node_down_history_is_updated_when_netsplit_happens,
        disco_node_up_timestamp_is_remembered,
        disco_node_down_timestamp_is_remembered,
        disco_nodeup_timestamp_is_updated_after_node_reconnects,
        disco_node_start_timestamp_is_updated_after_node_restarts,
        disco_late_pang_result_arrives_after_node_went_up
    ].

init_per_suite(Config) ->
    Names = [ct2, ct3, ct4, ct5],
    {Nodes, Peers} = lists:unzip([start_node(N) || N <- Names]),
    [
        {nodes, maps:from_list(lists:zip(Names, Nodes))},
        {peers, maps:from_list(lists:zip(Names, Peers))}
        | Config
    ].

end_per_suite(Config) ->
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
    ok.

%% Modules that use a multiline LOG_ macro
log_modules() ->
    [cets, cets_call, cets_long, cets_join, cets_discovery].

inserted_records_could_be_read_back(Config) ->
    Tab = make_name(Config),
    start_local(Tab),
    cets:insert(Tab, {alice, 32}),
    [{alice, 32}] = ets:lookup(Tab, alice).

insert_many_with_one_record(Config) ->
    Tab = make_name(Config),
    start_local(Tab),
    cets:insert_many(Tab, [{alice, 32}]),
    [{alice, 32}] = ets:lookup(Tab, alice).

insert_many_with_two_records(Config) ->
    Tab = make_name(Config),
    start_local(Tab),
    cets:insert_many(Tab, [{alice, 32}, {bob, 55}]),
    [{alice, 32}, {bob, 55}] = ets:tab2list(Tab).

delete_works(Config) ->
    Tab = make_name(Config),
    start_local(Tab),
    cets:insert(Tab, {alice, 32}),
    cets:delete(Tab, alice),
    [] = ets:lookup(Tab, alice).

delete_many_works(Config) ->
    Tab = make_name(Config, 1),
    start_local(Tab),
    cets:insert(Tab, {alice, 32}),
    cets:delete_many(Tab, [alice]),
    [] = ets:lookup(Tab, alice).

join_works(Config) ->
    given_two_joined_tables(Config).

inserted_records_could_be_read_back_from_replicated_table(Config) ->
    #{tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    cets:insert(Tab1, {alice, 32}),
    [{alice, 32}] = ets:lookup(Tab2, alice).

insert_new_works(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    true = cets:insert_new(Pid1, {alice, 32}),
    %% Duplicate found
    false = cets:insert_new(Pid1, {alice, 32}),
    false = cets:insert_new(Pid1, {alice, 33}),
    false = cets:insert_new(Pid2, {alice, 33}).

insert_new_works_with_table_name(Config) ->
    #{tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    true = cets:insert_new(Tab1, {alice, 32}),
    false = cets:insert_new(Tab2, {alice, 32}).

insert_new_works_when_leader_is_back(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    NotLeader = not_leader(Pid1, Pid2, Leader),
    cets:set_leader(Leader, false),
    spawn(fun() ->
        timer:sleep(100),
        cets:set_leader(Leader, true)
    end),
    true = cets:insert_new(NotLeader, {alice, 32}).

insert_new_when_new_leader_has_joined(Config) ->
    #{pids := Pids, tabs := Tabs} = given_3_servers(Config),
    %% Processes do not always start in order (i.e. sort them now)
    [Pid1, Pid2, Pid3] = lists:sort(Pids),
    %% Join first network segment
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    %% Pause insert into the first segment
    Leader = cets:get_leader(Pid1),
    PauseMon = cets:pause(Leader),
    spawn(fun() ->
        timer:sleep(100),
        ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid3),
        cets:unpause(Leader, PauseMon)
    end),
    %% Inserted by Pid3
    true = cets:insert_new(Pid1, {alice, 32}),
    Res = [{alice, 32}],
    [Res = cets:dump(T) || T <- Tabs].

%% Checks that the handle_wrong_leader is called
insert_new_when_new_leader_has_joined_duplicate(Config) ->
    #{pids := Pids, tabs := Tabs} = given_3_servers(Config),
    %% Processes do not always start in order (i.e. sort them now)
    [Pid1, Pid2, Pid3] = lists:sort(Pids),
    %% Join first network segment
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    %% Put record into the second network segment
    true = cets:insert_new(Pid3, {alice, 33}),
    %% Pause insert into the first segment
    Leader = cets:get_leader(Pid1),
    PauseMon = cets:pause(Leader),
    spawn(fun() ->
        timer:sleep(100),
        ok = cets_join:join(insert_new_lock5, #{}, Pid1, Pid3),
        cets:unpause(Leader, PauseMon)
    end),
    %% Checked and ignored by Pid3
    false = cets:insert_new(Pid1, {alice, 32}),
    Res = [{alice, 33}],
    [Res = cets:dump(T) || T <- Tabs].

insert_new_when_inconsistent_minimal(Config) ->
    #{pids := [Pid1, _Pid2]} = given_two_joined_tables(Config),
    true = cets:insert_new(Pid1, {alice, 33}),
    false = cets:insert_new(Pid1, {alice, 55}).

%% Rare case when tables contain different data
%% (the developer should try to avoid the manual removal of data if possible)
insert_new_when_inconsistent(Config) ->
    #{pids := [Pid1, Pid2]} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    NotLeader = not_leader(Pid1, Pid2, Leader),
    {ok, LeaderTab} = cets:table_name(Leader),
    {ok, NotLeaderTab} = cets:table_name(NotLeader),
    true = cets:insert_new(NotLeader, {alice, 33}),
    true = cets:insert_new(Leader, {bob, 40}),
    %% Introduce inconsistency
    ets:delete(NotLeaderTab, alice),
    ets:delete(LeaderTab, bob),
    false = cets:insert_new(NotLeader, {alice, 55}),
    true = cets:insert_new(Leader, {bob, 66}),
    [{bob, 40}] = cets:dump(NotLeaderTab),
    [{alice, 33}, {bob, 66}] = cets:dump(LeaderTab).

insert_new_is_retried_when_leader_is_reelected(Config) ->
    Me = self(),
    F = fun(X) ->
        put(test_stage, detected),
        Me ! {wrong_leader_detected, X}
    end,
    {ok, Pid1} = start_local(make_name(Config, 1), #{handle_wrong_leader => F}),
    {ok, Pid2} = start_local(make_name(Config, 2), #{handle_wrong_leader => F}),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    Leader = cets:get_leader(Pid1),
    NotLeader = not_leader(Pid1, Pid2, Leader),
    %% Ask process to reject all the leader operations
    cets:set_leader(Leader, false),
    spawn_link(fun() ->
        wait_till_test_stage(Leader, detected),
        %% Fix the leader, so it can process our insert_new call
        cets:set_leader(Leader, true)
    end),
    %% This function would block, because Leader process would reject the operation
    %% Until we call cets:set_leader(Leader, true)
    true = cets:insert_new(NotLeader, {alice, 32}),
    %% Check that we actually use retry logic
    %% Check that handle_wrong_leader callback function is called at least once
    receive
        {wrong_leader_detected, Info} ->
            ct:pal("wrong_leader_detected ~p", [Info])
    after 5000 ->
        ct:fail(wrong_leader_not_detected)
    end,
    %% Check that data is written (i.e. retry works)
    {ok, [{alice, 32}]} = cets:remote_dump(Pid1),
    {ok, [{alice, 32}]} = cets:remote_dump(Pid2).

%% We could retry automatically, but in this case return value from insert_new
%% could be incorrect.
%% If you want to make insert_new more robust:
%% - handle cets_down exception
%% - call insert_new one more time
%% - read the data back using ets:lookup to ensure it is your record written
insert_new_fails_if_the_leader_dies(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    cets:pause(Pid2),
    spawn(fun() ->
        timer:sleep(100),
        exit(Pid2, kill)
    end),
    try
        cets:insert_new(Pid1, {alice, 32})
    catch
        exit:{killed, _} -> ok
    end.

insert_new_fails_if_the_local_server_is_dead(_Config) ->
    Pid = stopped_pid(),
    try
        cets:insert_new(Pid, {alice, 32})
    catch
        exit:{noproc, {gen_server, call, _}} -> ok
    end.

insert_new_or_lookup_works(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    Rec1 = {alice, 32},
    Rec2 = {alice, 33},
    {true, [Rec1]} = cets:insert_new_or_lookup(Pid1, Rec1),
    %% Duplicate found
    {false, [Rec1]} = cets:insert_new_or_lookup(Pid1, Rec1),
    {false, [Rec1]} = cets:insert_new_or_lookup(Pid2, Rec1),
    {false, [Rec1]} = cets:insert_new_or_lookup(Pid1, Rec2),
    {false, [Rec1]} = cets:insert_new_or_lookup(Pid2, Rec2).

insert_serial_works(Config) ->
    #{pid1 := Pid1, tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    ok = cets:insert_serial(Pid1, {a, 1}),
    [{a, 1}] = cets:dump(Tab1),
    [{a, 1}] = cets:dump(Tab2).

insert_serial_overwrites_data(Config) ->
    #{pid1 := Pid1, tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    ok = cets:insert_serial(Pid1, {a, 1}),
    ok = cets:insert_serial(Pid1, {a, 2}),
    [{a, 2}] = cets:dump(Tab1),
    [{a, 2}] = cets:dump(Tab2).

%% Test case when both servers receive a request to update the same key.
%% Compare with insert_serial_overwrites_data_consistently
%% and insert_new_does_not_overwrite_data.
insert_overwrites_data_inconsistently(Config) ->
    Me = self(),
    #{pid1 := Pid1, pid2 := Pid2, tab1 := Tab1, tab2 := Tab2} =
        given_two_joined_tables(Config),
    spawn_link(fun() ->
        sys:replace_state(Pid1, fun(State) ->
            Me ! replacing_state1,
            receive_message(continue_test),
            State
        end)
    end),
    spawn_link(fun() ->
        sys:replace_state(Pid2, fun(State) ->
            Me ! replacing_state2,
            receive_message(continue_test),
            State
        end)
    end),
    receive_message(replacing_state1),
    receive_message(replacing_state2),
    %% Insert at the same time
    spawn_link(fun() ->
        ok = cets:insert(Tab1, {a, 1}),
        Me ! inserted1
    end),
    spawn_link(fun() ->
        ok = cets:insert(Tab2, {a, 2}),
        Me ! inserted2
    end),
    %% Wait till got an insert op in the queue
    wait_till_message_queue_length(Pid1, 1),
    wait_till_message_queue_length(Pid2, 1),
    Pid1 ! continue_test,
    Pid2 ! continue_test,
    receive_message(inserted1),
    receive_message(inserted2),
    %% Different values due to a race condition
    [{a, 2}] = cets:dump(Tab1),
    [{a, 1}] = cets:dump(Tab2).

insert_new_does_not_overwrite_data(Config) ->
    Me = self(),
    #{pid1 := Pid1, pid2 := Pid2, tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    spawn_link(fun() ->
        sys:replace_state(Pid1, fun(State) ->
            Me ! replacing_state1,
            receive_message(continue_test),
            State
        end)
    end),
    spawn_link(fun() ->
        sys:replace_state(Pid2, fun(State) ->
            Me ! replacing_state2,
            receive_message(continue_test),
            State
        end)
    end),
    receive_message(replacing_state1),
    receive_message(replacing_state2),
    %% Insert at the same time
    spawn_link(fun() ->
        true = cets:insert_new(Tab1, {a, 1}),
        Me ! inserted1
    end),
    wait_till_message_queue_length(Leader, 1),
    spawn_link(fun() ->
        false = cets:insert_new(Tab2, {a, 2}),
        Me ! inserted2
    end),
    %% Wait till got the insert ops in the queue.
    %% Leader gets both requests.
    wait_till_message_queue_length(Leader, 2),
    Pid1 ! continue_test,
    Pid2 ! continue_test,
    receive_message(inserted1),
    receive_message(inserted2),
    [{a, 1}] = cets:dump(Tab1),
    [{a, 1}] = cets:dump(Tab2).

%% We have to use table names instead of pids to insert, because
%% get_leader is an ETS call, if ServerRef is a table name.
%% And get_leader is a gen_server call, if ServerRef is a pid.
insert_serial_overwrites_data_consistently(Config) ->
    Me = self(),
    #{pid1 := Pid1, pid2 := Pid2, tab1 := Tab1, tab2 := Tab2} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    spawn_link(fun() ->
        sys:replace_state(Pid1, fun(State) ->
            Me ! replacing_state1,
            receive_message(continue_test),
            State
        end)
    end),
    spawn_link(fun() ->
        sys:replace_state(Pid2, fun(State) ->
            Me ! replacing_state2,
            receive_message(continue_test),
            State
        end)
    end),
    receive_message(replacing_state1),
    receive_message(replacing_state2),
    %% Insert at the same time
    spawn_link(fun() ->
        ok = cets:insert_serial(Tab1, {a, 1}),
        Me ! inserted1
    end),
    %% Ensure, that first insert comes before the second
    %% (just to get a predictable value. The value would be still
    %%  consistent in case first insert comes after the second).
    wait_till_message_queue_length(Leader, 1),
    spawn_link(fun() ->
        ok = cets:insert_serial(Tab2, {a, 2}),
        Me ! inserted2
    end),
    %% Wait till got the insert ops in the queue.
    %% Leader gets both requests.
    wait_till_message_queue_length(Leader, 2),
    Pid1 ! continue_test,
    Pid2 ! continue_test,
    receive_message(inserted1),
    receive_message(inserted2),
    [{a, 2}] = cets:dump(Tab1),
    [{a, 2}] = cets:dump(Tab2).

%% Similar to insert_new_works_when_leader_is_back
insert_serial_works_when_leader_is_back(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    NotLeader = not_leader(Pid1, Pid2, Leader),
    cets:set_leader(Leader, false),
    spawn(fun() ->
        timer:sleep(100),
        cets:set_leader(Leader, true)
    end),
    %% Blocks, until cets:set_leader sets leader back to true.
    ok = cets:insert_serial(NotLeader, {alice, 32}).

insert_serial_blocks_when_leader_is_not_back(Config) ->
    Me = self(),
    F = fun(X) ->
        put(test_stage, detected),
        Me ! {wrong_leader_detected, X}
    end,
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config, #{handle_wrong_leader => F}),
    Leader = cets:get_leader(Pid1),
    NotLeader = not_leader(Pid1, Pid2, Leader),
    cets:set_leader(Leader, false),
    InserterPid = spawn(fun() ->
        %% Will block indefinetely, because we set is_leader flag manually.
        ok = cets:insert_serial(NotLeader, {alice, 32})
    end),
    receive
        {wrong_leader_detected, Info} ->
            ct:log("wrong_leader_detected ~p", [Info])
    after 5000 ->
        ct:fail(wrong_leader_not_detected)
    end,
    %% Still alive and blocking
    pong = cets:ping(Pid1),
    pong = cets:ping(Pid2),
    ?assert(erlang:is_process_alive(InserterPid)).

leader_is_the_same_in_metadata_after_join(Config) ->
    #{tabs := [T1, T2], pids := [Pid1, Pid2]} = given_two_joined_tables(Config),
    Leader = cets:get_leader(Pid1),
    Leader = cets:get_leader(Pid2),
    Leader = cets_metadata:get(T1, leader),
    Leader = cets_metadata:get(T2, leader).

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
    cets_test_wait:wait_until(fun() -> count_remote_ops_in_the_message_box(Pid2) end, 1),
    sys:resume(Pid2),
    %% Wait till remote_op is processed
    cets:ping(Pid2),
    %% Check that the insert was ignored
    {ok, []} = cets:remote_dump(Pid2).

send_dump_fails_during_join_because_receiver_exits(Config) ->
    Me = self(),
    DownFn = fun(#{remote_pid := RemotePid, table := _Tab}) ->
        Me ! {down_called, self(), RemotePid}
    end,
    {ok, Pid1} = start_local(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = start_local(make_name(Config, 2), #{}),
    F = fun
        ({before_send_dump, P}) when P =:= Pid1 ->
            %% Kill Pid2 process.
            %% It does not crash the join process.
            %% Pid1 would receive a dump with Pid2 in the server list.
            exit(Pid2, sim_error),
            %% Ensure Pid1 got DOWN message from Pid2 already
            pong = cets:ping(Pid1),
            Me ! before_send_dump_called;
        (_) ->
            ok
    end,
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}),
    receive_message(before_send_dump_called),
    pong = cets:ping(Pid1),
    receive_message({down_called, Pid1, Pid2}),
    [] = cets:other_pids(Pid1),
    %% Pid1 still works
    cets:insert(Pid1, {1}),
    {ok, [{1}]} = cets:remote_dump(Pid1).

join_fails_in_check_fully_connected(Config) ->
    Me = self(),
    #{pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config),
    %% Pid2 and Pid3 are connected
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3, #{}),
    [Pid3] = cets:other_pids(Pid2),
    F = fun
        (before_check_fully_connected) ->
            %% Ask Pid2 to remove Pid3 from the list
            Pid2 ! {'DOWN', make_ref(), process, Pid3, sim_error},
            %% Ensure Pid2 did the cleaning
            pong = cets:ping(Pid2),
            [] = cets:other_pids(Pid2),
            Me ! before_check_fully_connected_called;
        (_) ->
            ok
    end,
    ?assertMatch(
        {error, {task_failed, check_fully_connected_failed, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F})
    ),
    receive_message(before_check_fully_connected_called).

join_fails_because_join_refs_do_not_match_for_nodes_in_segment(Config) ->
    #{pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config),
    %% Pid2 and Pid3 are connected
    %% But for some reason Pid3 has a different join_ref
    %% (probably could happen if it still haven't checked other nodes after a join)
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3, #{}),
    set_join_ref(Pid3, make_ref()),
    ?assertMatch(
        {error, {task_failed, check_same_join_ref_failed, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{})
    ).

join_fails_because_pids_do_not_match_for_nodes_in_segment(Config) ->
    #{pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config),
    %% Pid2 and Pid3 are connected
    %% But for some reason Pid3 has a different other_nodes list
    %% (probably could happen if it still haven't checked other nodes after a join)
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3, #{}),
    set_other_servers(Pid3, []),
    ?assertMatch(
        {error, {task_failed, check_fully_connected_failed, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{})
    ).

join_fails_because_servers_overlap(Config) ->
    #{pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config),
    set_other_servers(Pid1, [Pid3]),
    set_other_servers(Pid2, [Pid3]),
    ?assertMatch(
        {error, {task_failed, check_do_not_overlap_failed, #{}}},
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{})
    ).

%% join_fails_because_servers_overlap testcase, but we check the logging.
%% We check that `?LOG_ERROR(#{what => check_do_not_overlap_failed})' is called.
logs_are_printed_when_join_fails_because_servers_overlap(Config) ->
    LogRef = make_ref(),
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    #{pids := [Pid1, Pid2, Pid3]} = given_3_servers(Config),
    set_other_servers(Pid1, [Pid3]),
    set_other_servers(Pid2, [Pid3]),
    ?assertMatch(
        {error, {task_failed, check_do_not_overlap_failed, #{}}},
        cets_join:join(lock_name(Config), #{log_ref => LogRef}, Pid1, Pid2, #{})
    ),
    receive
        {log, ?FUNCTION_NAME, #{
            level := error,
            msg :=
                {report, #{
                    what := check_do_not_overlap_failed, log_ref := LogRef
                }}
        }} ->
            ok
    after 5000 ->
        ct:fail(timeout)
    end.

remote_ops_are_ignored_if_join_ref_does_not_match(Config) ->
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}),
    #{join_ref := JoinRef} = cets:info(Pid1),
    set_join_ref(Pid1, make_ref()),
    cets:insert(Pid2, {1}),
    %% fix and check again
    set_join_ref(Pid1, JoinRef),
    cets:insert(Pid2, {2}),
    {ok, [{2}]} = cets:remote_dump(Pid1).

join_retried_if_lock_is_busy(Config) ->
    Me = self(),
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    Lock = lock_name(Config),
    SleepyF = fun
        (join_start) ->
            Me ! join_start,
            timer:sleep(infinity);
        (_) ->
            ok
    end,
    F = fun
        (before_retry) -> Me ! before_retry;
        (_) -> ok
    end,
    %% Get the lock in a separate process
    spawn_link(fun() ->
        cets_join:join(Lock, #{}, Pid1, Pid2, #{checkpoint_handler => SleepyF})
    end),
    receive_message(join_start),
    %% We actually would not return from cets_join:join unless we get the lock
    spawn_link(fun() ->
        ok = cets_join:join(Lock, #{}, Pid1, Pid2, #{checkpoint_handler => F})
    end),
    receive_message(before_retry).

join_done_already_while_waiting_for_lock_so_do_nothing(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Me = self(),
    #{pids := [Pid1, Pid2, Pid3, Pid4]} = given_n_servers(Config, 4, #{}),
    Lock = lock_name(Config),
    ok = cets_join:join(Lock, #{}, Pid1, Pid2, #{}),
    ok = cets_join:join(Lock, #{}, Pid3, Pid4, #{}),
    %% It is to just match logs
    LogRef = make_ref(),
    Info = #{log_ref => LogRef},
    F1 = send_join_start_back_and_wait_for_continue_joining(),
    F2 = fun(_) -> ok end,
    %% Get the lock in a separate process
    spawn_link(fun() ->
        ok = cets_join:join(Lock, Info, Pid1, Pid3, #{checkpoint_handler => F1}),
        Me ! first_join_returns
    end),
    JoinPid = receive_message_with_arg(join_start),
    spawn_link(fun() ->
        ok = cets_join:join(Lock, Info, Pid1, Pid3, #{checkpoint_handler => F2}),
        Me ! second_join_returns
    end),
    JoinPid ! continue_joining,
    %% At this point our first join would finish, after that our second join should exit too.
    receive_message(first_join_returns),
    receive_message(second_join_returns),
    %% Ensure all logs are received by removing the handler, it is a sync operation.
    %% (we do not expect any logs anyway).
    logger:remove_handler(?FUNCTION_NAME),
    %% Ensure there is nothing logged, we use log_ref to ignore logs from other tests.
    %% The counter example for no logging is
    %% the logs_are_printed_when_join_fails_because_servers_overlap testcase.
    assert_nothing_is_logged(?FUNCTION_NAME, LogRef).

atom_error_is_logged_in_tracked(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    LogRef = make_ref(),
    F = fun() -> error(oops) end,
    ?assertException(
        error,
        {task_failed, oops, #{log_ref := LogRef}},
        cets_long:run_tracked(#{log_ref => LogRef}, F)
    ),
    [
        #{
            level := error,
            msg :=
                {report, #{
                    what := task_failed,
                    log_ref := LogRef,
                    reason := oops
                }}
        }
    ] =
        receive_all_logs_with_log_ref(?FUNCTION_NAME, LogRef).

shutdown_reason_is_not_logged_in_tracked(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Me = self(),
    LogRef = make_ref(),
    F = fun() ->
        Me ! ready,
        timer:sleep(infinity)
    end,
    Pid = spawn(fun() -> cets_long:run_tracked(#{log_ref => LogRef}, F) end),
    receive_message(ready),
    exit(Pid, shutdown),
    wait_for_down(Pid),
    [] = receive_all_logs_with_log_ref(?FUNCTION_NAME, LogRef).

%% Complementary to shutdown_reason_is_not_logged_in_tracked
other_reason_is_logged_in_tracked(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Me = self(),
    LogRef = make_ref(),
    F = fun() ->
        Me ! ready,
        timer:sleep(infinity)
    end,
    Pid = spawn(fun() -> cets_long:run_tracked(#{log_ref => LogRef}, F) end),
    receive_message(ready),
    exit(Pid, bad_stuff_happened),
    wait_for_down(Pid),
    [
        #{
            level := error,
            msg :=
                {report, #{
                    what := task_failed,
                    log_ref := LogRef,
                    reason := bad_stuff_happened
                }}
        }
    ] = receive_all_logs_with_log_ref(?FUNCTION_NAME, LogRef).

nested_calls_errors_are_logged_once_with_tuple_reason(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    LogRef = make_ref(),
    F = fun() -> error({something_is_wrong, ?FUNCTION_NAME}) end,
    FF = fun() -> cets_long:run_tracked(#{log_ref => LogRef, task => subtask}, F) end,
    ?assertException(
        error,
        {task_failed, {something_is_wrong, nested_calls_errors_are_logged_once_with_tuple_reason},
            #{log_ref := LogRef}},
        cets_long:run_tracked(#{log_ref => LogRef, task => main}, FF)
    ),
    [
        #{
            level := error,
            msg :=
                {report, #{
                    what := task_failed,
                    log_ref := LogRef,
                    reason := {something_is_wrong, ?FUNCTION_NAME}
                }}
        }
    ] =
        receive_all_logs_with_log_ref(?FUNCTION_NAME, LogRef).

nested_calls_errors_are_logged_once_with_map_reason(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    LogRef = make_ref(),
    F = fun() -> error(#{something_is_wrong => ?FUNCTION_NAME}) end,
    FF = fun() -> cets_long:run_tracked(#{log_ref => LogRef, task => subtask}, F) end,
    ?assertException(
        error,
        {task_failed,
            #{
                something_is_wrong :=
                    nested_calls_errors_are_logged_once_with_map_reason
            },
            #{log_ref := LogRef}},
        cets_long:run_tracked(#{log_ref => LogRef, task => main}, FF)
    ),
    [
        #{
            level := error,
            msg :=
                {report, #{
                    what := task_failed,
                    log_ref := LogRef,
                    reason := #{something_is_wrong := ?FUNCTION_NAME}
                }}
        }
    ] =
        receive_all_logs_with_log_ref(?FUNCTION_NAME, LogRef).

send_dump_contains_already_added_servers(Config) ->
    %% Check that even if we have already added server in send_dump, nothing crashes
    {ok, Pid1} = start_local(make_name(Config, 1)),
    {ok, Pid2} = start_local(make_name(Config, 2)),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}),
    PauseRef = cets:pause(Pid1),
    %% That should be called by cets_join module
    cets:send_dump(Pid1, [Pid2], make_ref(), [{1}]),
    cets:unpause(Pid1, PauseRef),
    {ok, [{1}]} = cets:remote_dump(Pid1).

test_multinode(Config) ->
    Node1 = node(),
    #{ct2 := Peer2, ct3 := Peer3, ct4 := Peer4} = proplists:get_value(peers, Config),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Peer2, Tab),
    {ok, Pid3} = start(Peer3, Tab),
    {ok, Pid4} = start(Peer4, Tab),
    ok = join(Node1, Tab, Pid1, Pid3),
    ok = join(Peer2, Tab, Pid2, Pid4),
    insert(Node1, Tab, {a}),
    insert(Peer2, Tab, {b}),
    insert(Peer3, Tab, {c}),
    insert(Peer4, Tab, {d}),
    [{a}, {c}] = dump(Node1, Tab),
    [{b}, {d}] = dump(Peer2, Tab),
    ok = join(Node1, Tab, Pid2, Pid1),
    [{a}, {b}, {c}, {d}] = dump(Node1, Tab),
    [{a}, {b}, {c}, {d}] = dump(Peer2, Tab),
    insert(Node1, Tab, {f}),
    insert(Peer4, Tab, {e}),
    Same = fun(X) ->
        X = dump(Node1, Tab),
        X = dump(Peer2, Tab),
        X = dump(Peer3, Tab),
        X = dump(Peer4, Tab),
        ok
    end,
    Same([{a}, {b}, {c}, {d}, {e}, {f}]),
    delete(Node1, Tab, e),
    Same([{a}, {b}, {c}, {d}, {f}]),
    delete(Peer4, Tab, a),
    Same([{b}, {c}, {d}, {f}]),
    %% Bulk operations are supported
    insert_many(Peer4, Tab, [{m}, {a}, {n}, {y}]),
    Same([{a}, {b}, {c}, {d}, {f}, {m}, {n}, {y}]),
    delete_many(Peer4, Tab, [a, n]),
    Same([{b}, {c}, {d}, {f}, {m}, {y}]),
    ok.

test_multinode_remote_insert(Config) ->
    Tab = make_name(Config),
    #{ct2 := Node2, ct3 := Node3} = proplists:get_value(nodes, Config),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    ok = join(Node2, Tab, Pid2, Pid3),
    %% Ensure it is a remote node
    true = node() =/= node(Pid2),
    %% Insert without calling rpc module
    cets:insert(Pid2, {a}),
    [{a}] = dump(Node3, Tab).

node_list_is_correct(Config) ->
    Node1 = node(),
    #{ct2 := Node2, ct3 := Node3, ct4 := Node4} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    {ok, Pid4} = start(Node4, Tab),
    ok = join(Node1, Tab, Pid1, Pid3),
    ok = join(Node2, Tab, Pid2, Pid4),
    ok = join(Node1, Tab, Pid1, Pid2),
    [Node2, Node3, Node4] = other_nodes(Node1, Tab),
    [Node1, Node3, Node4] = other_nodes(Node2, Tab),
    [Node1, Node2, Node4] = other_nodes(Node3, Tab),
    [Node1, Node2, Node3] = other_nodes(Node4, Tab),
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
    {ok, Disco} = cets_discovery:start(#{tables => [Tab], disco_file => FileName}),
    %% Disco is async, so we have to wait for the final state
    ok = cets_discovery:wait_for_ready(Disco, 5000),
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
    {ok, Disco} = cets_discovery:start(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    %% Disco is async, so we have to wait for the final state
    ok = cets_discovery:wait_for_ready(Disco, 5000),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco),
    ok.

test_disco_delete_table(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    Tab = make_name(Config),
    cets_discovery:add_table(Disco, Tab),
    #{tables := [Tab]} = cets_discovery:system_info(Disco),
    cets_discovery:delete_table(Disco, Tab),
    #{tables := []} = cets_discovery:system_info(Disco).

test_disco_delete_unknown_table(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    Tab = make_name(Config),
    cets_discovery:delete_table(Disco, Tab),
    #{tables := []} = cets_discovery:system_info(Disco).

test_disco_delete_table_twice(Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
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
    {ok, Disco} = cets_discovery:start(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end,
        after_error
    ),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    %% Disco is async, so we have to wait for the final state
    ok = cets_discovery:wait_for_ready(Disco, 5000),
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
    {ok, Disco} = cets_discovery:start(#{tables => [], disco_file => FileName}),
    cets_discovery:add_table(Disco, Tab),
    %% Check that wait_for_ready would not block forever:
    ok = cets_discovery:wait_for_ready(Disco, 5000),
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
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(Disco, 5000),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := Tab}] =
        cets_discovery:info(Disco).

test_disco_add_table_twice(Config) ->
    Dir = proplists:get_value(priv_dir, Config),
    FileName = filename:join(Dir, "disco.txt"),
    {ok, Disco} = cets_discovery:start(#{tables => [], disco_file => FileName}),
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
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
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
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end,
        after_error
    ),
    ok.

disco_uses_regular_retry_interval_in_the_regular_phase(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    F = fun(State) -> {{ok, [Node1, Node2]}, State} end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    Disco ! enter_regular_phase,
    cets_discovery:add_table(Disco, Tab),
    cets_test_wait:wait_until(
        fun() -> maps:get(last_get_nodes_retry_type, cets_discovery:system_info(Disco)) end, regular
    ),
    #{phase := regular} = cets_discovery:system_info(Disco).

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
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    cets_discovery:add_table(Disco, Tab),
    %% get_nodes call is async, so wait for it
    cets_test_wait:wait_until(
        fun() -> length(maps:get(nodes, cets_discovery:system_info(Disco))) end,
        3
    ),
    Disco ! {nodeup, BadNode},
    Disco ! {nodedown, BadNode},
    %% Check that wait_for_ready still works
    ok = cets_discovery:wait_for_ready(Disco, 5000).

status_available_nodes(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, []}, State}
    end,
    DiscoName = disco_name(Config),
    start_disco(Node1, #{name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F}),
    start_disco(Node2, #{name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F}),
    ?assertMatch(#{available_nodes := [Node1, Node2]}, cets_status:status(DiscoName)).

status_available_nodes_do_not_contain_nodes_with_stopped_disco(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    start_disco(Node1, #{name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F}),
    %% Disco not running
    ?assertMatch(#{available_nodes := [Node1]}, cets_status:status(DiscoName)).

status_unavailable_nodes(Config) ->
    Node1 = node(),
    F = fun(State) ->
        {{ok, [Node1, 'badnode@localhost']}, State}
    end,
    DiscoName = disco_name(Config),
    Disco = start_disco(Node1, #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    %% Disco needs at least one table to start calling get_nodes function
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{unavailable_nodes := ['badnode@localhost']}, cets_status:status(DiscoName)).

status_unavailable_nodes_is_subset_of_discovery_nodes(Config) ->
    Node1 = node(),
    GetFn1 = fun(State) -> {{ok, [Node1, 'badnode@localhost']}, State} end,
    GetFn2 = fun(State) -> {{ok, [Node1]}, State} end,
    %% Setup meck
    BackendModule = make_name(Config, disco_backend),
    meck:new(BackendModule, [non_strict]),
    meck:expect(BackendModule, init, fun(_Opts) -> undefined end),
    meck:expect(BackendModule, get_nodes, GetFn1),
    DiscoName = disco_name(Config),
    Disco = start_disco(Node1, #{
        name => DiscoName, backend_module => BackendModule
    }),
    %% Disco needs at least one table to start calling get_nodes function
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{unavailable_nodes := ['badnode@localhost']}, cets_status:status(DiscoName)),
    %% Remove badnode from disco
    meck:expect(BackendModule, get_nodes, GetFn2),
    %% Force check.
    Disco ! check,
    %% The unavailable_nodes list is updated
    CondF = fun() -> maps:get(unavailable_nodes, cets_status:status(DiscoName)) end,
    cets_test_wait:wait_until(CondF, []).

status_joined_nodes(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco1 = start_disco(Node1, DiscoOpts),
    Disco2 = start_disco(Node2, DiscoOpts),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    %% Add table using pids (i.e. no need to do RPCs here)
    cets_discovery:add_table(Disco1, Tab),
    cets_discovery:add_table(Disco2, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    cets_test_wait:wait_until(fun() -> maps:get(joined_nodes, cets_status:status(DiscoName)) end, [
        Node1, Node2
    ]).

status_discovery_works(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco1 = start_disco(Node1, DiscoOpts),
    Disco2 = start_disco(Node2, DiscoOpts),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    %% Add table using pids (i.e. no need to do RPCs here)
    cets_discovery:add_table(Disco1, Tab),
    cets_discovery:add_table(Disco2, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{discovery_works := true}, cets_status:status(DiscoName)).

status_discovered_nodes(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    Disco = start_disco(Node1, #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Node2, Tab),
    %% Add table using pids (i.e. no need to do RPCs here)
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{discovered_nodes := [Node1, Node2]}, cets_status:status(DiscoName)).

status_remote_nodes_without_disco(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    Disco = start_disco(Node1, #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    }),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{remote_nodes_without_disco := [Node2]}, cets_status:status(DiscoName)).

status_remote_nodes_with_unknown_tables(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco1 = start_disco(Node1, DiscoOpts),
    Disco2 = start_disco(Node2, DiscoOpts),
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    %% Node1 does not have Tab2
    {ok, _} = start(Node1, Tab2),
    {ok, _} = start(Node2, Tab1),
    {ok, _} = start(Node2, Tab2),
    %% Add table using pids (i.e. no need to do RPCs here)
    cets_discovery:add_table(Disco1, Tab1),
    cets_discovery:add_table(Disco2, Tab1),
    cets_discovery:add_table(Disco2, Tab2),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    cets_test_wait:wait_until(
        fun() -> maps:get(remote_nodes_with_unknown_tables, cets_status:status(DiscoName)) end, [
            Node2
        ]
    ),
    cets_test_wait:wait_until(
        fun() -> maps:get(remote_unknown_tables, cets_status:status(DiscoName)) end, [
            Tab2
        ]
    ).

status_remote_nodes_with_missing_nodes(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco1 = start_disco(Node1, DiscoOpts),
    Disco2 = start_disco(Node2, DiscoOpts),
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    %% Node2 does not have Tab2
    {ok, _} = start(Node1, Tab1),
    {ok, _} = start(Node1, Tab2),
    {ok, _} = start(Node2, Tab1),
    cets_discovery:add_table(Disco1, Tab1),
    cets_discovery:add_table(Disco1, Tab2),
    cets_discovery:add_table(Disco2, Tab1),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    cets_test_wait:wait_until(
        fun() -> maps:get(remote_nodes_with_missing_tables, cets_status:status(DiscoName)) end, [
            Node2
        ]
    ),
    cets_test_wait:wait_until(
        fun() -> maps:get(remote_missing_tables, cets_status:status(DiscoName)) end, [
            Tab2
        ]
    ).

status_conflict_nodes(Config) ->
    Node1 = node(),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    F = fun(State) ->
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco1 = start_disco(Node1, DiscoOpts),
    Disco2 = start_disco(Node2, DiscoOpts),
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, _} = start(Node1, Tab1),
    {ok, _} = start(Node1, Tab2),
    {ok, _} = start(Node2, Tab1),
    {ok, Pid22} = start(Node2, Tab2),
    cets_discovery:add_table(Disco1, Tab1),
    cets_discovery:add_table(Disco1, Tab2),
    cets_discovery:add_table(Disco2, Tab1),
    cets_discovery:add_table(Disco2, Tab2),
    ok = cets_discovery:wait_for_ready(DiscoName, 5000),
    set_other_servers(Pid22, []),
    cets_test_wait:wait_until(
        fun() -> maps:get(conflict_nodes, cets_status:status(DiscoName)) end, [Node2]
    ),
    cets_test_wait:wait_until(
        fun() -> maps:get(conflict_tables, cets_status:status(DiscoName)) end, [Tab2]
    ).

disco_wait_for_get_nodes_works(_Config) ->
    F = fun(State) -> {{ok, []}, State} end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    ok = cets_discovery:wait_for_get_nodes(Disco, 5000).

disco_wait_for_get_nodes_blocks_and_returns(Config) ->
    Tab = make_name(Config, 1),
    {ok, _Pid} = start_local(Tab, #{}),
    SignallingPid = make_signalling_process(),
    F = fun(State) ->
        wait_for_down(SignallingPid),
        {{ok, []}, State}
    end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
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
    {ok, Disco} = cets_discovery:start(#{
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

get_nodes_request(Config) ->
    #{ct2 := Node2, ct3 := Node3, ct4 := Node4} = proplists:get_value(nodes, Config),
    Tab = make_name(Config),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    {ok, Pid4} = start(Node4, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3),
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid4),
    Res = cets:wait_response(cets:get_nodes_request(Pid2), 5000),
    ?assertEqual({reply, [Node2, Node3, Node4]}, Res).

test_locally(Config) ->
    #{tabs := [T1, T2]} = given_two_joined_tables(Config),
    cets:insert(T1, {1}),
    cets:insert(T1, {1}),
    cets:insert(T2, {2}),
    D = cets:dump(T1),
    D = cets:dump(T2).

handle_down_is_called(Config) ->
    Parent = self(),
    DownFn = fun(#{remote_pid := _RemotePid, table := _Tab}) ->
        Parent ! down_called
    end,
    {ok, Pid1} = start_local(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = start_local(make_name(Config, 2), #{}),
    ok = cets_join:join(lock_name(Config), #{table => [d1, d2]}, Pid1, Pid2),
    exit(Pid2, oops),
    receive
        down_called -> ok
    after 5000 -> ct:fail(timeout)
    end.

events_are_applied_in_the_correct_order_after_unpause(Config) ->
    T = make_name(Config),
    {ok, Pid} = start_local(T),
    PauseMon = cets:pause(Pid),
    R1 = cets:insert_request(T, {1}),
    R2 = cets:delete_request(T, 1),
    cets:delete_request(T, 2),
    cets:insert_request(T, {2}),
    cets:insert_request(T, {3}),
    cets:insert_request(T, {4}),
    cets:insert_request(T, {5}),
    R3 = cets:insert_request(T, [{6}, {7}]),
    R4 = cets:delete_many_request(T, [5, 4]),
    [] = lists:sort(cets:dump(T)),
    ok = cets:unpause(Pid, PauseMon),
    [{reply, ok} = cets:wait_response(R, 5000) || R <- [R1, R2, R3, R4]],
    [{2}, {3}, {6}, {7}] = lists:sort(cets:dump(T)).

pause_multiple_times(Config) ->
    T = make_name(Config),
    {ok, Pid} = start_local(T),
    PauseMon1 = cets:pause(Pid),
    PauseMon2 = cets:pause(Pid),
    Ref1 = cets:insert_request(Pid, {1}),
    Ref2 = cets:insert_request(Pid, {2}),
    %% No records yet, even after pong
    [] = cets:dump(T),
    ok = cets:unpause(Pid, PauseMon1),
    pong = cets:ping(Pid),
    %% No records yet, even after pong
    [] = cets:dump(T),
    ok = cets:unpause(Pid, PauseMon2),
    pong = cets:ping(Pid),
    {reply, ok} = cets:wait_response(Ref1, 5000),
    {reply, ok} = cets:wait_response(Ref2, 5000),
    [{1}, {2}] = lists:sort(cets:dump(T)).

unpause_twice(Config) ->
    T = make_name(Config),
    {ok, Pid} = start_local(T),
    PauseMon = cets:pause(Pid),
    ok = cets:unpause(Pid, PauseMon),
    {error, unknown_pause_monitor} = cets:unpause(Pid, PauseMon).

unpause_if_pause_owner_crashes(Config) ->
    Me = self(),
    {ok, Pid} = start_local(make_name(Config)),
    spawn_monitor(fun() ->
        cets:pause(Pid),
        Me ! pause_called,
        error(wow)
    end),
    receive
        pause_called -> ok
    after 5000 -> ct:fail(timeout)
    end,
    %% Check that the server is unpaused
    ok = cets:insert(Pid, {1}).

write_returns_if_remote_server_crashes(Config) ->
    #{tab1 := Tab1, pid2 := Pid2} = given_two_joined_tables(Config),
    sys:suspend(Pid2),
    R = cets:insert_request(Tab1, {1}),
    exit(Pid2, oops),
    {reply, ok} = cets:wait_response(R, 5000).

ack_process_stops_correctly(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    #{ack_pid := AckPid} = cets:info(Pid),
    AckMon = monitor(process, AckPid),
    cets:stop(Pid),
    receive
        {'DOWN', AckMon, process, AckPid, normal} -> ok
    after 5000 -> ct:fail(timeout)
    end.

ack_process_handles_unknown_remote_server(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    sys:suspend(Pid2),
    #{ack_pid := AckPid} = cets:info(Pid1),
    [Pid2] = cets:other_pids(Pid1),
    RandomPid = spawn(fun() -> ok end),
    %% Request returns immediately,
    %% we actually need to send a ping to ensure it has been processed locally
    R = cets:insert_request(Pid1, {1}),
    pong = cets:ping(Pid1),
    %% Extract From value
    [{servers, _}, {From, [Pid2]}] = maps:to_list(sys:get_state(AckPid)),
    cets_ack:ack(AckPid, From, RandomPid),
    sys:resume(Pid2),
    %% Ack process still works fine
    {reply, ok} = cets:wait_response(R, 5000).

ack_process_handles_unknown_from(Config) ->
    #{pid1 := Pid1} = given_two_joined_tables(Config),
    #{ack_pid := AckPid} = cets:info(Pid1),
    R = cets:insert_request(Pid1, {1}),
    From = {self(), make_ref()},
    cets_ack:ack(AckPid, From, self()),
    %% Ack process still works fine
    {reply, ok} = cets:wait_response(R, 5000).

ack_calling_add_when_server_list_is_empty_is_not_allowed(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    Mon = monitor(process, Pid),
    #{ack_pid := AckPid} = cets:info(Pid),
    FakeFrom = {self(), make_ref()},
    cets_ack:add(AckPid, FakeFrom),
    %% cets server would never send an add message in the single node configuration
    %% (cets_ack is not used if there is only one node,
    %% so cets module calls gen_server:reply and skips the replication)
    receive
        {'DOWN', Mon, process, Pid, Reason} ->
            {unexpected_add_msg, _} = Reason
    after 5000 -> ct:fail(timeout)
    end.

ping_all_using_name_works(Config) ->
    T = make_name(Config),
    {ok, _Pid1} = start_local(T),
    cets:ping_all(T).

insert_many_request(Config) ->
    Tab = make_name(Config),
    {ok, Pid} = start_local(Tab),
    R = cets:insert_many_request(Pid, [{a}, {b}]),
    {reply, ok} = cets:wait_response(R, 5000),
    [{a}, {b}] = ets:tab2list(Tab).

insert_many_requests(Config) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1),
    {ok, Pid2} = start_local(Tab2),
    R1 = cets:insert_many_request(Pid1, [{a}, {b}]),
    R2 = cets:insert_many_request(Pid2, [{a}, {b}]),
    [{reply, ok}, {reply, ok}] = cets:wait_responses([R1, R2], 5000).

insert_many_requests_timeouts(Config) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1),
    {ok, Pid2} = start_local(Tab2),
    cets:pause(Pid1),
    R1 = cets:insert_many_request(Pid1, [{a}, {b}]),
    R2 = cets:insert_many_request(Pid2, [{a}, {b}]),
    %% We assume 100 milliseconds is more than enough to insert one record
    %% (it is time sensitive testcase though)
    [timeout, {reply, ok}] = cets:wait_responses([R1, R2], 100).

insert_into_bag(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{type => bag}),
    cets:insert(T, {1, 1}),
    cets:insert(T, {1, 1}),
    cets:insert(T, {1, 2}),
    [{1, 1}, {1, 2}] = lists:sort(cets:dump(T)).

delete_from_bag(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}]),
    cets:delete_object(T, {1, 2}),
    [{1, 1}] = cets:dump(T).

delete_many_from_bag(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}, {1, 3}, {1, 5}, {2, 3}]),
    cets:delete_objects(T, [{1, 2}, {1, 5}, {1, 4}]),
    [{1, 1}, {1, 3}, {2, 3}] = lists:sort(cets:dump(T)).

delete_request_from_bag(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}]),
    R = cets:delete_object_request(T, {1, 2}),
    {reply, ok} = cets:wait_response(R, 5000),
    [{1, 1}] = cets:dump(T).

delete_request_many_from_bag(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}, {1, 3}]),
    R = cets:delete_objects_request(T, [{1, 1}, {1, 3}]),
    {reply, ok} = cets:wait_response(R, 5000),
    [{1, 2}] = cets:dump(T).

insert_into_bag_is_replicated(Config) ->
    #{pid1 := Pid1, tab2 := T2} = given_two_joined_tables(Config, #{type => bag}),
    cets:insert(Pid1, {1, 1}),
    [{1, 1}] = cets:dump(T2).

insert_into_keypos_table(Config) ->
    T = make_name(Config),
    {ok, _Pid} = start_local(T, #{keypos => 2}),
    cets:insert(T, {rec, 1}),
    cets:insert(T, {rec, 2}),
    [{rec, 1}] = lists:sort(ets:lookup(T, 1)),
    [{rec, 1}, {rec, 2}] = lists:sort(cets:dump(T)).

table_name_works(Config) ->
    T = make_name(Config),
    {ok, Pid} = start_local(T),
    {ok, T} = cets:table_name(T),
    {ok, T} = cets:table_name(Pid),
    #{table := T} = cets:info(Pid).

info_contains_opts(Config) ->
    T = make_name(Config),
    {ok, Pid} = start_local(T, #{type => bag}),
    #{opts := #{type := bag}} = cets:info(Pid).

check_could_reach_each_other_fails(_Config) ->
    ?assertException(
        error,
        check_could_reach_each_other_failed,
        cets_join:check_could_reach_each_other(#{}, [self()], [bad_node_pid()])
    ).

%% Cases to improve code coverage

unknown_down_message_is_ignored(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    RandPid = spawn(fun() -> ok end),
    Pid ! {'DOWN', make_ref(), process, RandPid, oops},
    still_works(Pid).

unknown_message_is_ignored(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    Pid ! oops,
    still_works(Pid).

unknown_cast_message_is_ignored(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    gen_server:cast(Pid, oops),
    still_works(Pid).

unknown_message_is_ignored_in_ack_process(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    #{ack_pid := AckPid} = cets:info(Pid),
    AckPid ! oops,
    still_works(Pid).

unknown_cast_message_is_ignored_in_ack_process(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    #{ack_pid := AckPid} = cets:info(Pid),
    gen_server:cast(AckPid, oops),
    still_works(Pid).

unknown_call_returns_error_from_ack_process(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    #{ack_pid := AckPid} = cets:info(Pid),
    {error, unexpected_call} = gen_server:call(AckPid, oops),
    still_works(Pid).

code_change_returns_ok(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    sys:suspend(Pid),
    ok = sys:change_code(Pid, cets, v2, []),
    sys:resume(Pid).

code_change_returns_ok_for_ack(Config) ->
    {ok, Pid} = start_local(make_name(Config)),
    #{ack_pid := AckPid} = cets:info(Pid),
    sys:suspend(AckPid),
    ok = sys:change_code(AckPid, cets_ack, v2, []),
    sys:resume(AckPid).

run_spawn_forwards_errors(_Config) ->
    ?assertException(
        error,
        {task_failed, oops, #{}},
        cets_long:run_spawn(#{}, fun() -> error(oops) end)
    ).

run_tracked_failed(_Config) ->
    F = fun() -> error(oops) end,
    ?assertException(
        error,
        {task_failed, oops, #{}},
        cets_long:run_tracked(#{}, F)
    ).

run_tracked_logged(_Config) ->
    F = fun() -> timer:sleep(100) end,
    cets_long:run_tracked(#{report_interval => 10}, F).

run_tracked_logged_check_logger(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    LogRef = make_ref(),
    F = fun() -> timer:sleep(infinity) end,
    %% Run it in a separate process, so we can check logs in the test process
    %% Overwrite default five seconds interval with 10 milliseconds
    spawn_link(fun() -> cets_long:run_tracked(#{report_interval => 10, log_ref => LogRef}, F) end),
    %% Exit test after first log event
    receive
        {log, ?FUNCTION_NAME, #{
            level := warning,
            msg := {report, #{what := long_task_progress, log_ref := LogRef}}
        }} ->
            ok
    after 5000 ->
        ct:fail(timeout)
    end.

%% Improves code coverage, checks logs
long_call_fails_because_linked_process_dies(_Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    LogRef = make_ref(),
    Me = self(),
    F = fun() ->
        Me ! task_started,
        timer:sleep(infinity)
    end,
    RunPid = spawn(fun() -> cets_long:run_tracked(#{log_ref => LogRef}, F) end),
    %% To avoid race conditions
    receive_message(task_started),
    spawn(fun() ->
        link(RunPid),
        error(sim_error_in_linked_process)
    end),
    wait_for_down(RunPid),
    %% Exit test after first log event
    receive
        {log, ?FUNCTION_NAME, #{
            level := error,
            msg := {report, #{what := task_failed, log_ref := LogRef, caller_pid := RunPid}}
        }} ->
            ok
    after 5000 ->
        ct:fail(timeout)
    end.

long_call_to_unknown_name_throws_pid_not_found(_Config) ->
    ?assertException(
        error,
        {pid_not_found, unknown_name_please},
        cets_call:long_call(unknown_name_please, test)
    ).

send_leader_op_throws_noproc(_Config) ->
    ?assertException(
        exit,
        {noproc, {gen_server, call, [unknown_name_please, get_leader]}},
        cets_call:send_leader_op(unknown_name_please, {op, {insert, {1}}})
    ).

pinfo_returns_value(_Config) ->
    true = is_list(cets_long:pinfo(self(), messages)).

pinfo_returns_undefined(_Config) ->
    undefined = cets_long:pinfo(stopped_pid(), messages).

%% Netsplit cases (run in sequence)

insert_returns_when_netsplit(Config) ->
    #{ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    Node1 = node(),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid5} = start(Peer5, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid5),
    sys:suspend(Pid5),
    R = cets:insert_request(Tab, {1, test}),
    block_node(Node5, Peer5),
    try
        {reply, ok} = cets:wait_response(R, 5000)
    after
        reconnect_node(Node5, Peer5)
    end.

inserts_after_netsplit_reconnects(Config) ->
    #{ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    Node1 = node(),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid5} = start(Peer5, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid5),
    sys:suspend(Pid5),
    R = cets:insert_request(Tab, {1, v1}),
    block_node(Node5, Peer5),
    try
        {reply, ok} = cets:wait_response(R, 5000)
    after
        reconnect_node(Node5, Peer5)
    end,
    sys:resume(Pid5),
    cets:insert(Pid1, {1, v2}),
    cets:insert(Pid5, {1, v3}),
    %% No automatic recovery
    [{1, v2}] = dump(Node1, Tab),
    [{1, v3}] = dump(Peer5, Tab).

disco_connects_to_unconnected_node(Config) ->
    Node1 = node(),
    #{ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    ok = net_kernel:monitor_nodes(true),
    disconnect_node(Peer5, Node1),
    receive_message({nodedown, Node5}),
    Tab = make_name(Config),
    {ok, _} = start(Node1, Tab),
    {ok, _} = start(Peer5, Tab),
    F = fun(State) ->
        {{ok, [Node1, Node5]}, State}
    end,
    {ok, Disco} = cets_discovery:start(#{backend_module => cets_discovery_fun, get_nodes_fn => F}),
    cets_discovery:add_table(Disco, Tab),
    ok = cets_discovery:wait_for_ready(Disco, 5000).

%% Joins from a bad (not fully connected) node
%% Join process should check if nodes could contact each other before allowing to join
joining_not_fully_connected_node_is_not_allowed(Config) ->
    #{ct3 := Peer3, ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    Node1 = node(),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid3} = start(Peer3, Tab),
    {ok, Pid5} = start(Peer5, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid3),
    %% No connection between Peer5 and Node1
    block_node(Node5, Peer5),
    try
        %% Pid5 and Pid3 could contact each other.
        %% Pid3 could contact Pid1 (they are joined).
        %% But Pid5 cannot contact Pid1.
        {error, {task_failed, check_could_reach_each_other_failed, _}} =
            rpc(Peer5, cets_join, join, [lock_name(Config), #{}, Pid5, Pid3]),
        %% Still connected
        cets:insert(Pid1, {r1}),
        {ok, [{r1}]} = cets:remote_dump(Pid3),
        [Pid3] = cets:other_pids(Pid1),
        [Pid1] = cets:other_pids(Pid3)
    after
        reconnect_node(Node5, Peer5)
    end,
    [] = cets:other_pids(Pid5).

%% Joins from a good (fully connected) node
joining_not_fully_connected_node_is_not_allowed2(Config) ->
    #{ct3 := Peer3, ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    Node1 = node(),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid3} = start(Peer3, Tab),
    {ok, Pid5} = start(Peer5, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid3),
    %% No connection between Peer5 and Node1
    block_node(Node5, Peer5),
    try
        %% Pid5 and Pid3 could contact each other.
        %% Pid3 could contact Pid1 (they are joined).
        %% But Pid5 cannot contact Pid1.
        {error, {task_failed, check_could_reach_each_other_failed, _}} = rpc(
            Peer3, cets_join, join, [
                lock_name(Config), #{}, Pid5, Pid3
            ]
        ),
        %% Still connected
        cets:insert(Pid1, {r1}),
        {ok, [{r1}]} = cets:remote_dump(Pid3),
        [Pid3] = cets:other_pids(Pid1),
        [Pid1] = cets:other_pids(Pid3)
    after
        reconnect_node(Node5, Peer5)
    end,
    [] = cets:other_pids(Pid5).

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
        Logs = receive_all_logs(?FUNCTION_NAME),
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

cets_ping_all_returns_when_ping_crashes(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    meck:new(cets, [passthrough]),
    meck:expect(cets_call, long_call, fun
        (Server, ping) when Server == Pid2 -> error(simulate_crash);
        (Server, Msg) -> meck:passthrough([Server, Msg])
    end),
    ?assertMatch({error, [{Pid2, {'EXIT', {simulate_crash, _}}}]}, cets:ping_all(Pid1)),
    meck:unload().

join_interrupted_when_ping_crashes(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    Tab3 = make_name(Config, 3),
    {ok, Pid3} = start_local(Tab3, #{}),
    meck:new(cets, [passthrough]),
    meck:expect(cets_call, long_call, fun
        (Server, ping) when Server == Pid2 -> error(simulate_crash);
        (Server, Msg) -> meck:passthrough([Server, Msg])
    end),
    Res = cets_join:join(lock_name(Config), #{}, Pid1, Pid3),
    ?assertMatch({error, {task_failed, ping_all_failed, #{}}}, Res),
    meck:unload().

node_down_history_is_updated_when_netsplit_happens(Config) ->
    %% node_down_history is available in cets:info/1 API.
    %% It could be used for manual debugging in situations
    %% we get netsplits or during rolling upgrades.
    #{ct5 := Peer5} = proplists:get_value(peers, Config),
    #{ct5 := Node5} = proplists:get_value(nodes, Config),
    Node1 = node(),
    Tab = make_name(Config),
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid5} = start(Peer5, Tab),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid5),
    block_node(Node5, Peer5),
    try
        F = fun() -> maps:get(node_down_history, cets:info(Pid1)) end,
        cets_test_wait:wait_until(F, [Node5])
    after
        reconnect_node(Node5, Peer5),
        cets:stop(Pid5)
    end.

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
    %% At this point cets_disco should reconnect nodes back automatically.
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
    #{disco := Disco, node1 := Node1, node2 := Node2, peer2 := Peer2} = Setup,
    OldTimestamp = get_disco_timestamp(Disco, nodeup_timestamps, Node2),
    disconnect_node(Peer2, Node1),
    wait_for_disco_timestamp_to_be_updated(Disco, nodeup_timestamps, Node2, OldTimestamp).

disco_node_start_timestamp_is_updated_after_node_restarts(Config) ->
    logger_debug_h:start(#{id => ?FUNCTION_NAME}),
    Setup = setup_two_nodes_and_discovery(Config, [wait, disco2]),
    #{disco := Disco, node2 := Node2} = Setup,
    OldTimestamp = get_disco_timestamp(Disco, node_start_timestamps, Node2),
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

format_data_does_not_return_table_duplicates(Config) ->
    Res = cets_status:format_data(test_data_for_duplicate_missing_table_in_status(Config)),
    ?assertMatch(#{remote_unknown_tables := [], remote_nodes_with_missing_tables := []}, Res).

cets_ping_non_existing_node(_Config) ->
    pang = cets_ping:ping('mongooseim@non_existing_host').

pre_connect_fails_on_our_node(_Config) ->
    mock_epmd(),
    %% We would fail to connect to the remote EPMD but we would get an IP
    pang = cets_ping:ping('mongooseim@resolvabletobadip'),
    meck:unload().

pre_connect_fails_on_one_of_the_nodes(Config) ->
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    mock_epmd(),
    %% We would get pong on Node2, but would fail an RPC to our hode
    pang = rpc(Node2, cets_ping, ping, ['cetsnode1@localhost']),
    History = meck:history(erl_epmd),
    %% Check that Node2 called us
    ?assertMatch(
        [_],
        [
            X
         || {_, {erl_epmd, address_please, ["cetsnode1", "localhost", inet]},
                {ok, {192, 168, 100, 134}}} = X <- History
        ],
        History
    ),
    meck:unload().

cets_ping_net_family(_Config) ->
    inet = cets_ping:net_family(error),
    inet = cets_ping:net_family({ok, [["inet"]]}),
    inet6 = cets_ping:net_family({ok, [["inet6"]]}),
    inet6 = cets_ping:net_family({ok, [["inet6_tls"]]}).

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

ping_pairs_returns_pongs(Config) ->
    #{ct2 := Node2, ct3 := Node3} = proplists:get_value(nodes, Config),
    Me = node(),
    [{Me, Node2, pong}, {Node2, Node3, pong}] =
        cets_ping:ping_pairs([{Me, Node2}, {Node2, Node3}]).

ping_pairs_returns_earlier(Config) ->
    #{ct2 := Node2, ct3 := Node3} = proplists:get_value(nodes, Config),
    Me = node(),
    Bad = 'badnode@localhost',
    [{Me, Me, pong}, {Me, Node2, pong}, {Me, Bad, pang}, {Me, Node3, skipped}] =
        cets_ping:ping_pairs([{Me, Me}, {Me, Node2}, {Me, Bad}, {Me, Node3}]).

%% Helper functions

receive_all_logs(Id) ->
    receive
        {log, Id, Log} ->
            [Log | receive_all_logs(Id)]
    after 100 ->
        []
    end.

still_works(Pid) ->
    pong = cets:ping(Pid),
    %% The server works fine
    ok = cets:insert(Pid, {1}),
    {ok, [{1}]} = cets:remote_dump(Pid).

start_local(Name) ->
    start_local(Name, #{}).

start_local(Name, Opts) ->
    catch cets:stop(Name),
    wait_for_name_to_be_free(node(), Name),
    {ok, Pid} = cets:start(Name, Opts),
    schedule_cleanup(Pid),
    {ok, Pid}.

schedule_cleanup(Pid) ->
    Me = self(),
    spawn(fun() ->
        Ref = erlang:monitor(process, Me),
        receive
            {'DOWN', Ref, process, Me, _} ->
                cets:stop(Pid)
        end
    end).

start(Node, Tab) ->
    catch rpc(Node, cets, stop, [Tab]),
    wait_for_name_to_be_free(Node, Tab),
    {ok, Pid} = rpc(Node, cets, start, [Tab, #{}]),
    schedule_cleanup(Pid),
    {ok, Pid}.

start_disco(Node, Opts) ->
    case Opts of
        #{name := Name} ->
            catch rpc(Node, cets, stop, [Name]),
            wait_for_name_to_be_free(Node, Name);
        _ ->
            ok
    end,
    {ok, Pid} = rpc(Node, cets_discovery, start, [Opts]),
    schedule_cleanup(Pid),
    Pid.

wait_for_name_to_be_free(Node, Name) ->
    %% Wait for the old process to be killed by the cleaner in schedule_cleanup.
    %% Cleaner is fast, but not instant.
    cets_test_wait:wait_until(fun() -> rpc(Node, erlang, whereis, [Name]) end, undefined).

insert(Node, Tab, Rec) ->
    rpc(Node, cets, insert, [Tab, Rec]).

insert_many(Node, Tab, Records) ->
    rpc(Node, cets, insert_many, [Tab, Records]).

delete(Node, Tab, Key) ->
    rpc(Node, cets, delete, [Tab, Key]).

delete_many(Node, Tab, Keys) ->
    rpc(Node, cets, delete_many, [Tab, Keys]).

dump(Node, Tab) ->
    rpc(Node, cets, dump, [Tab]).

other_nodes(Node, Tab) ->
    rpc(Node, cets, other_nodes, [Tab]).

join(Node1, Tab, Pid1, Pid2) ->
    rpc(Node1, cets_join, join, [lock1, #{table => Tab}, Pid1, Pid2]).

%% Apply function using rpc or peer module
rpc(Peer, M, F, Args) when is_pid(Peer) ->
    case peer:call(Peer, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end;
rpc(Node, M, F, Args) when is_atom(Node) ->
    case rpc:call(Node, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end.

%% Set epmd_port for better coverage
extra_args(ct2) -> ["-epmd_port", "4369"];
extra_args(ct5) -> ["-kernel", "prevent_overlapping_partitions", "false"];
extra_args(_) -> "".

start_node(Sname) ->
    {ok, Peer, Node} = ?CT_PEER(#{
        name => Sname, connection => standard_io, args => extra_args(Sname)
    }),
    %% Keep nodes running after init_per_suite is finished
    unlink(Peer),
    %% Do RPC using alternative connection method
    ok = peer:call(Peer, code, add_paths, [code:get_path()]),
    {Node, Peer}.

receive_message(M) ->
    receive
        M -> ok
    after 5000 -> error({receive_message_timeout, M})
    end.

receive_message_with_arg(Tag) ->
    receive
        {Tag, Arg} -> Arg
    after 5000 -> error({receive_message_with_arg_timeout, Tag})
    end.

flush_message(M) ->
    receive
        M ->
            flush_message(M)
    after 0 ->
        ok
    end.

make_name(Config) ->
    make_name(Config, 1).

make_name(Config, Num) when is_integer(Num) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ integer_to_list(Num));
make_name(Config, Atom) when is_atom(Atom) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ atom_to_list(Atom)).

lock_name(Config) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_lock").

disco_name(Config) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_disco").

count_remote_ops_in_the_message_box(Pid) ->
    {messages, Messages} = erlang:process_info(Pid, messages),
    Ops = [M || M <- Messages, element(1, M) =:= remote_op],
    length(Ops).

set_join_ref(Pid, JoinRef) ->
    sys:replace_state(Pid, fun(#{join_ref := _} = State) -> State#{join_ref := JoinRef} end).

set_other_servers(Pid, Servers) ->
    sys:replace_state(Pid, fun(#{other_servers := _} = State) ->
        State#{other_servers := Servers}
    end).

given_two_joined_tables(Config) ->
    given_two_joined_tables(Config, #{}).

given_two_joined_tables(Config, Opts) ->
    Tab1 = make_name(Config, 1),
    Tab2 = make_name(Config, 2),
    {ok, Pid1} = start_local(Tab1, Opts),
    {ok, Pid2} = start_local(Tab2, Opts),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2),
    #{
        tab1 => Tab1,
        tab2 => Tab2,
        pid1 => Pid1,
        pid2 => Pid2,
        tabs => [Tab1, Tab2],
        pids => [Pid1, Pid2]
    }.

given_3_servers(Config) ->
    given_3_servers(Config, #{}).

given_3_servers(Config, Opts) ->
    given_n_servers(Config, 3, Opts).

given_n_servers(Config, N, Opts) ->
    Tabs = [make_name(Config, X) || X <- lists:seq(1, N)],
    Pids = [
        begin
            {ok, Pid} = start_local(Tab, Opts),
            Pid
        end
     || Tab <- Tabs
    ],
    #{pids => Pids, tabs => Tabs}.

setup_two_nodes_and_discovery(Config) ->
    setup_two_nodes_and_discovery(Config, []).

%% Flags:
%% - disco2 - start discovery on Node2
%% - wait - call wait_for_ready/2
setup_two_nodes_and_discovery(Config, Flags) ->
    ok = net_kernel:monitor_nodes(true),
    Me = self(),
    Node1 = node(),
    #{ct2 := Peer2} = proplists:get_value(peers, Config),
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    disconnect_node(Peer2, Node1),
    receive_message({nodedown, Node2}),
    Tab = make_name(Config),
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Peer2, Tab),
    F = fun(State) ->
        case lists:member(notify_get_nodes, Flags) of
            true ->
                Me ! get_nodes;
            false ->
                ok
        end,
        {{ok, [Node1, Node2]}, State}
    end,
    DiscoName = disco_name(Config),
    DiscoOpts = #{
        name => DiscoName, backend_module => cets_discovery_fun, get_nodes_fn => F
    },
    Disco = start_disco(Node1, DiscoOpts),
    %% Start Disco on second node (it is not always needed)
    Res =
        case lists:member(disco2, Flags) of
            true ->
                Disco2 = start_disco(Node2, DiscoOpts),
                cets_discovery:add_table(Disco2, Tab),
                #{disco2 => Disco2};
            false ->
                #{}
        end,
    cets_discovery:add_table(Disco, Tab),
    case lists:member(wait, Flags) of
        true ->
            try
                ok = cets_discovery:wait_for_ready(Disco, 5000)
            catch
                Class:Reason:Stacktrace ->
                    ct:pal("system_info: ~p", [cets_discovery:system_info(Disco)]),
                    erlang:raise(Class, Reason, Stacktrace)
            end;
        false ->
            ok
    end,
    case lists:member(netsplit, Flags) of
        true ->
            %% Simulate a loss of connection between nodes
            disconnect_node(Peer2, Node1);
        false ->
            ok
    end,
    Res#{
        disco_name => DiscoName,
        disco_opts => DiscoOpts,
        disco => Disco,
        node1 => Node1,
        node2 => Node2,
        peer2 => Peer2
    }.

simulate_disco_restart(#{
    disco_opts := DiscoOpts,
    disco2 := Disco2,
    node1 := Node1,
    node2 := Node2,
    peer2 := Peer2
}) ->
    %% Instead of restart the node, restart the process. It is enough to get
    %% a new start_time.
    disconnect_node(Peer2, Node1),
    rpc(Peer2, cets, stop, [Disco2]),
    %% We actually would not detect the case of us just stopping the remote disco
    %% server. Because we use nodeup/nodedown to detect downs, not monitors.
    _RestartedDisco2 = start_disco(Node2, DiscoOpts).

stopped_pid() ->
    %% Get a pid for a stopped process
    {Pid, Mon} = spawn_monitor(fun() -> ok end),
    receive
        {'DOWN', Mon, process, Pid, _Reason} -> ok
    end,
    Pid.

get_pd(Pid, Key) ->
    {dictionary, Dict} = erlang:process_info(Pid, dictionary),
    proplists:get_value(Key, Dict).

wait_till_test_stage(Pid, Stage) ->
    cets_test_wait:wait_until(fun() -> get_pd(Pid, test_stage) end, Stage).

wait_till_message_queue_length(Pid, Len) ->
    cets_test_wait:wait_until(fun() -> get_message_queue_length(Pid) end, Len).

get_message_queue_length(Pid) ->
    {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
    Len.

wait_for_down(Pid) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, process, Pid, Reason} -> Reason
    after 5000 -> ct:fail({wait_for_down_timeout, Pid})
    end.

%% Disconnect node until manually connected
block_node(Node, Peer) when is_atom(Node), is_pid(Peer) ->
    rpc(Peer, erlang, set_cookie, [node(), invalid_cookie]),
    disconnect_node(Peer, node()),
    %% Wait till node() is notified about the disconnect
    cets_test_wait:wait_until(fun() -> rpc(Peer, net_adm, ping, [node()]) end, pang),
    cets_test_wait:wait_until(fun() -> rpc(node(), net_adm, ping, [Node]) end, pang).

reconnect_node(Node, Peer) when is_atom(Node), is_pid(Peer) ->
    rpc(Peer, erlang, set_cookie, [node(), erlang:get_cookie()]),
    %% Very rarely it could return pang
    cets_test_wait:wait_until(fun() -> rpc(Peer, net_adm, ping, [node()]) end, pong),
    cets_test_wait:wait_until(fun() -> rpc(node(), net_adm, ping, [Node]) end, pong).

disconnect_node(RPCNode, DisconnectNode) ->
    rpc(RPCNode, erlang, disconnect_node, [DisconnectNode]).

not_leader(Leader, Other, Leader) ->
    Other;
not_leader(Other, Leader, Leader) ->
    Other.

bad_node_pid() ->
    binary_to_term(bad_node_pid_binary()).

bad_node_pid_binary() ->
    %% Pid <0.90.0> on badnode@localhost
    <<131, 88, 100, 0, 17, 98, 97, 100, 110, 111, 100, 101, 64, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 0, 90, 0, 0, 0, 0, 100, 206, 70, 92>>.

assert_nothing_is_logged(LogHandlerId, LogRef) ->
    receive
        {log, LogHandlerId, #{
            level := Level,
            msg := {report, #{log_ref := LogRef}}
        }} when Level =:= warning; Level =:= error ->
            ct:fail(got_logging_but_should_not)
    after 0 ->
        ok
    end.

send_join_start_back_and_wait_for_continue_joining() ->
    Me = self(),
    fun
        (join_start) ->
            Me ! {join_start, self()},
            receive
                continue_joining ->
                    ok
            end;
        (_) ->
            ok
    end.

receive_all_logs_with_log_ref(LogHandlerId, LogRef) ->
    ?LOG_ERROR(#{what => ensure_nothing_logged_before, log_ref => LogRef}),
    receive
        {log, LogHandlerId, #{
            msg := {report, #{log_ref := LogRef, what := ensure_nothing_logged_before}}
        }} ->
            ok
    after 5000 ->
        ct:fail({timeout, logger_is_broken})
    end,
    %% Do a new logging call to check that it is the only log message
    ?LOG_ERROR(#{what => ensure_nothing_logged_after, log_ref => LogRef}),
    %% We only match messages with the matching log_ref here
    %% to ignore messages from the other parallel tests
    receive
        {log, LogHandlerId, Log = #{msg := {report, Report = #{log_ref := LogRef}}}} ->
            case Report of
                #{what := ensure_nothing_logged_after} ->
                    [];
                _ ->
                    [Log | receive_all_logs_with_log_ref(LogHandlerId, LogRef)]
            end
    after 5000 ->
        ct:fail({timeout, receive_all_logs_with_log_ref})
    end.

%% Gathered after Helm update
%% with cets_status:gather_data(mongoose_cets_discovery).
test_data_for_duplicate_missing_table_in_status(Config) ->
    %% Create atoms in non sorted order
    %% maps:keys returns keys in the atom-creation order (and not sorted).
    %% Also, compiler is smart and would optimize list_to_atom("literal_string"),
    %% so we do a module call to disable this optimization.
    _ = list_to_atom(?MODULE:return_same("cets_external_component")),
    _ = list_to_atom(?MODULE:return_same("cets_bosh")),
    Name = filename:join(proplists:get_value(data_dir, Config), "status_data.txt"),
    {ok, [Term]} = file:consult(Name),
    Term.

return_same(X) ->
    X.

wait_for_disco_timestamp_to_appear(Disco, MapName, NodeKey) ->
    F = fun() ->
        #{MapName := Map} = cets_discovery:system_info(Disco),
        maps:is_key(NodeKey, Map)
    end,
    cets_test_wait:wait_until(F, true).

wait_for_disco_timestamp_to_be_updated(Disco, MapName, NodeKey, OldTimestamp) ->
    Cond = fun() ->
        NewTimestamp = get_disco_timestamp(Disco, MapName, NodeKey),
        NewTimestamp =/= OldTimestamp
    end,
    cets_test_wait:wait_until(Cond, true).

get_disco_timestamp(Disco, MapName, NodeKey) ->
    Info = cets_discovery:system_info(Disco),
    #{MapName := #{NodeKey := Timestamp}} = Info,
    Timestamp.

make_signalling_process() ->
    spawn_link(fun() ->
        receive
            stop -> ok
        end
    end).

mock_epmd() ->
    meck:new(erl_epmd, [passthrough, unstick]),
    meck:expect(erl_epmd, address_please, fun
        ("cetsnode1", "localhost", inet) -> {ok, {192, 168, 100, 134}};
        (Name, Host, Family) -> meck:passthrough([Name, Host, Family])
    end).
