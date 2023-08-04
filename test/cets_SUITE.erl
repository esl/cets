-module(cets_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
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
        insert_new_when_inconsistent,
        insert_new_is_retried_when_leader_is_reelected,
        insert_new_fails_if_the_leader_dies,
        insert_new_fails_if_the_local_server_is_dead,
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
        sync_using_name_works,
        insert_many_request,
        insert_into_bag,
        delete_from_bag,
        delete_many_from_bag,
        delete_request_from_bag,
        delete_request_many_from_bag,
        insert_into_bag_is_replicated,
        insert_into_keypos_table,
        table_name_works,
        info_contains_opts,
        unknown_down_message_is_ignored,
        unknown_message_is_ignored,
        unknown_cast_message_is_ignored,
        unknown_message_is_ignored_in_ack_process,
        unknown_cast_message_is_ignored_in_ack_process,
        unknown_call_returns_error_from_ack_process,
        code_change_returns_ok,
        code_change_returns_ok_for_ack,
        run_spawn_forwards_errors
    ].

init_per_suite(Config) ->
    Node2 = start_node(ct2),
    Node3 = start_node(ct3),
    Node4 = start_node(ct4),
    [{nodes, [Node2, Node3, Node4]} | Config].

end_per_suite(Config) ->
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

inserted_records_could_be_read_back(_Config) ->
    cets:start(ins1, #{}),
    cets:insert(ins1, {alice, 32}),
    [{alice, 32}] = ets:lookup(ins1, alice).

insert_many_with_one_record(_Config) ->
    cets:start(ins1m, #{}),
    cets:insert_many(ins1m, [{alice, 32}]),
    [{alice, 32}] = ets:lookup(ins1m, alice).

insert_many_with_two_records(_Config) ->
    cets:start(ins2m, #{}),
    cets:insert_many(ins2m, [{alice, 32}, {bob, 55}]),
    [{alice, 32}, {bob, 55}] = ets:tab2list(ins2m).

delete_works(_Config) ->
    cets:start(del1, #{}),
    cets:insert(del1, {alice, 32}),
    cets:delete(del1, alice),
    [] = ets:lookup(del1, alice).

delete_many_works(_Config) ->
    cets:start(del1, #{}),
    cets:insert(del1, {alice, 32}),
    cets:delete_many(del1, [alice]),
    [] = ets:lookup(del1, alice).

join_works(_Config) ->
    {ok, Pid1} = cets:start(join1tab, #{}),
    {ok, Pid2} = cets:start(join2tab, #{}),
    ok = cets_join:join(join_lock1, #{}, Pid1, Pid2).

inserted_records_could_be_read_back_from_replicated_table(_Config) ->
    {ok, Pid1} = cets:start(ins1tab, #{}),
    {ok, Pid2} = cets:start(ins2tab, #{}),
    ok = cets_join:join(join_lock1_ins, #{}, Pid1, Pid2),
    cets:insert(ins1tab, {alice, 32}),
    [{alice, 32}] = ets:lookup(ins2tab, alice).

insert_new_works(_Config) ->
    {ok, Pid1} = cets:start(newins1tab, #{}),
    {ok, Pid2} = cets:start(newins2tab, #{}),
    ok = cets_join:join(join_lock1_insnew, #{}, Pid1, Pid2),
    true = cets:insert_new(Pid1, {alice, 32}),
    %% Duplicate found
    false = cets:insert_new(Pid1, {alice, 32}),
    false = cets:insert_new(Pid1, {alice, 33}),
    false = cets:insert_new(Pid2, {alice, 33}).

insert_new_works_with_table_name(_Config) ->
    {ok, Pid1} = cets:start(T1 = tabinsnew1, #{}),
    {ok, Pid2} = cets:start(T2 = tabinsnew2, #{}),
    ok = cets_join:join(join_lock1_insnew2, #{}, Pid1, Pid2),
    true = cets:insert_new(T1, {alice, 32}),
    false = cets:insert_new(T2, {alice, 32}).

insert_new_works_when_leader_is_back(_Config) ->
    {ok, Pid1} = cets:start(newins1tab_back, #{}),
    {ok, Pid2} = cets:start(newins2tab_back, #{}),
    ok = cets_join:join(join_lock1_insnew_back, #{}, Pid1, Pid2),
    Leader = cets:get_leader(Pid1),
    %% Highest Pid is the leader:
    Pid2 = Leader,
    cets:set_leader(Leader, false),
    spawn(fun() ->
        timer:sleep(100),
        cets:set_leader(Leader, true)
    end),
    true = cets:insert_new(Pid1, {alice, 32}).

insert_new_when_new_leader_has_joined(_Config) ->
    {ok, Pid1} = cets:start(T1 = insert_new_tab4a, #{}),
    {ok, Pid2} = cets:start(T2 = insert_new_tab4b, #{}),
    {ok, Pid3} = cets:start(T3 = insert_new_tab4c, #{}),
    %% Join first network segment
    ok = cets_join:join(insert_new_lock4, #{}, Pid1, Pid2),
    %% Pause insert into the first segment
    Leader = cets:get_leader(Pid1),
    PauseMon = cets:pause(Leader),
    spawn(fun() ->
        timer:sleep(100),
        ok = cets_join:join(insert_new_lock4, #{}, Pid1, Pid3),
        cets:unpause(Leader, PauseMon)
    end),
    %% Inserted by Pid3
    true = cets:insert_new(Pid1, {alice, 32}),
    Res = [{alice, 32}],
    [Res = cets:dump(T) || T <- [T1, T2, T3]].

%% Checks that the handle_wrong_leader is called
insert_new_when_new_leader_has_joined_duplicate(_Config) ->
    {ok, Pid1} = cets:start(T1 = insert_new_tab5a, #{}),
    {ok, Pid2} = cets:start(T2 = insert_new_tab5b, #{}),
    {ok, Pid3} = cets:start(T3 = insert_new_tab5c, #{}),
    %% Join first network segment
    ok = cets_join:join(join_lock1_insnew_back4, #{}, Pid1, Pid2),
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
    [Res = cets:dump(T) || T <- [T1, T2, T3]].

%% Rare case when tables contain different data
%% (the developer should try to avoid the manual removal of data if possible)
insert_new_when_inconsistent(_Config) ->
    {ok, Pid1} = cets:start(T1 = insert_new_lock6a, #{}),
    {ok, Pid2} = cets:start(T2 = insert_new_lock6b, #{}),
    ok = cets_join:join(insert_new_lock6, #{}, Pid1, Pid2),
    true = cets:insert_new(Pid1, {alice, 33}),
    true = cets:insert_new(Pid2, {bob, 40}),
    %% Introduce inconsistency
    ets:delete(T1, alice),
    ets:delete(T2, bob),
    false = cets:insert_new(Pid1, {alice, 55}),
    true = cets:insert_new(Pid2, {bob, 66}),
    [{bob, 40}] = cets:dump(T1),
    [{alice, 33}, {bob, 66}] = cets:dump(T2).

insert_new_is_retried_when_leader_is_reelected(_Config) ->
    Me = self(),
    F = fun(X) -> Me ! {wrong_leader_detected, X} end,
    {ok, Pid1} = cets:start(newins1tab_back2, #{}),
    {ok, Pid2} = cets:start(newins2tab_back2, #{handle_wrong_leader => F}),
    ok = cets_join:join(join_lock1_insnew_back2, #{}, Pid1, Pid2),
    Leader = cets:get_leader(Pid1),
    %% Ask process to reject all the leader operations
    cets:set_leader(Leader, false),
    spawn(fun() ->
        timer:sleep(100),
        %% Fix the leader, so it can process our insert_new call
        cets:set_leader(Leader, true)
    end),
    %% This function would block, because Leader process would reject the operation
    %% Until we call cets:set_leader(Leader, true)
    true = cets:insert_new(Pid1, {alice, 32}),
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
%% If you wanna make insert_new more robust:
%% - handle cets_down exception
%% - call insert_new one more time
%% - read the data back using ets:lookup to ensure it is your record written
insert_new_fails_if_the_leader_dies(_Config) ->
    {ok, Pid1} = cets:start(newins1tab_back3, #{}),
    {ok, Pid2} = cets:start(newins2tab_back3, #{}),
    ok = cets_join:join(join_lock1_insnew_back3, #{}, Pid1, Pid2),
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
    %% Get a pid for a stopped process
    {Pid, Mon} = spawn_monitor(fun() -> ok end),
    receive
        {'DOWN', Mon, process, Pid, _Reason} -> ok
    end,
    try
        cets:insert_new(Pid, {alice, 32})
    catch
        exit:{noproc, {gen_server, call, _}} -> ok
    end.

leader_is_the_same_in_metadata_after_join(_Config) ->
    {ok, Pid1} = cets:start(T1 = check_lead1, #{}),
    {ok, Pid2} = cets:start(T2 = check_lead2, #{}),
    ok = cets_join:join(check_lead_lock, #{}, Pid1, Pid2),
    Leader = cets:get_leader(Pid1),
    Leader = cets:get_leader(Pid2),
    Leader = cets_metadata:get(T1, leader),
    Leader = cets_metadata:get(T2, leader).

join_works_with_existing_data(_Config) ->
    {ok, Pid1} = cets:start(ex1tab, #{}),
    {ok, Pid2} = cets:start(ex2tab, #{}),
    cets:insert(ex1tab, {alice, 32}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(join_lock1_ex, #{}, Pid1, Pid2),
    [{alice, 32}] = ets:lookup(ex2tab, alice).

%% This testcase tests an edgecase: inserting with the same key from two nodes.
%% Usually, inserting with the same key from two different nodes is not possible
%% (because the node-name is a part of the key).
join_works_with_existing_data_with_conflicts(_Config) ->
    {ok, Pid1} = cets:start(con1tab, #{}),
    {ok, Pid2} = cets:start(con2tab, #{}),
    cets:insert(con1tab, {alice, 32}),
    cets:insert(con2tab, {alice, 33}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(join_lock1_con, #{}, Pid1, Pid2),
    %% We insert data from other table into our table when merging, so the values get swapped
    [{alice, 33}] = ets:lookup(con1tab, alice),
    [{alice, 32}] = ets:lookup(con2tab, alice).

join_works_with_existing_data_with_conflicts_and_defined_conflict_handler(_Config) ->
    Opts = #{handle_conflict => fun resolve_highest/2},
    {ok, Pid1} = cets:start(fn_con1tab, Opts),
    {ok, Pid2} = cets:start(fn_con2tab, Opts),
    cets:insert(fn_con1tab, {alice, 32}),
    cets:insert(fn_con2tab, {alice, 33}),
    %% Join will copy and merge existing tables
    ok = cets_join:join(join_lock2_con, #{}, Pid1, Pid2),
    %% Key with the highest Number remains
    [{alice, 33}] = ets:lookup(fn_con1tab, alice),
    [{alice, 33}] = ets:lookup(fn_con2tab, alice).

join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_more_keys(_Config) ->
    %% Deeper testing of cets_join:apply_resolver function
    Opts = #{handle_conflict => fun resolve_highest/2},
    {ok, Pid1} = cets:start(T1 = fn2_con1tab, Opts),
    {ok, Pid2} = cets:start(T2 = fn2_con2tab, Opts),
    {ok, Pid3} = cets:start(T3 = fn2_con3tab, Opts),
    cets:insert_many(T1, [{alice, 32}, {bob, 10}, {michal, 40}]),
    cets:insert_many(T2, [{alice, 33}, {kate, 3}, {michal, 2}]),
    %% Join will copy and merge existing tables
    ok = cets_join:join(join_lock3_con, #{}, Pid1, Pid2),
    ok = cets_join:join(join_lock3_con, #{}, Pid1, Pid3),
    %% Key with the highest Number remains
    Dump = [{alice, 33}, {bob, 10}, {kate, 3}, {michal, 40}],
    Dump = cets:dump(T1),
    Dump = cets:dump(T2),
    Dump = cets:dump(T3).

-record(user, {name, age, updated}).

%% Test with records (which require keypos = 2 option)
join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_keypos2(_Config) ->
    Opts = #{handle_conflict => fun resolve_user_conflict/2, keypos => 2},
    {ok, Pid1} = cets:start(T1 = keypos2_tab1, Opts),
    {ok, Pid2} = cets:start(T2 = keypos2_tab2, Opts),
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

bag_with_conflict_handler_not_allowed(_Config) ->
    {error, [bag_with_conflict_handler]} =
        cets:start(ex1tab, #{handle_conflict => fun resolve_highest/2, type => bag}).

join_with_the_same_pid(_Config) ->
    {ok, Pid} = cets:start(joinsame, #{}),
    %% Just insert something into a table to check later the size
    cets:insert(joinsame, {1, 1}),
    link(Pid),
    {error, join_with_the_same_pid} = cets_join:join(joinsame_lock1_con, #{}, Pid, Pid),
    Nodes = [node()],
    %% The process is still running and no data loss (i.e. size is not zero)
    #{nodes := Nodes, size := 1} = cets:info(Pid).

join_ref_is_same_after_join(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}),
    #{join_ref := JoinRef} = cets:info(Pid1),
    #{join_ref := JoinRef} = cets:info(Pid2).

join_fails_because_server_process_not_found(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    F = fun
        (join_start) ->
            exit(Pid1, sim_error);
        (_) ->
            ok
    end,
    {error, {noproc, {gen_server, call, [Pid1, get_info, infinity]}}} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}).

join_fails_because_server_process_not_found_before_get_pids(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    F = fun
        (before_get_pids) ->
            exit(Pid1, sim_error);
        (_) ->
            ok
    end,
    {error, {noproc, {gen_server, call, [Pid1, other_servers, infinity]}}} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}).

join_fails_before_send_dump(Config) ->
    Me = self(),
    DownFn = fun(#{remote_pid := RemotePid, table := _Tab}) ->
        Me ! {down_called, self(), RemotePid}
    end,
    {ok, Pid1} = cets:start(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
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
    {error, sim_error} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}),
    %% Ensure we sent dump to Pid1
    receive_message(before_send_dump_called_for_pid1),
    %% Not joined, some data exchanged
    cets:sync(Pid1),
    cets:sync(Pid2),
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
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
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
    {error, sim_error2} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}),
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
    {ok, Pid1} = cets:start(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
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
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    {ok, Pid3} = cets:start(make_name(Config, 3), #{}),
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
    {error, check_fully_connected_failed} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{checkpoint_handler => F}),
    receive_message(before_check_fully_connected_called).

join_fails_because_join_refs_do_not_match_for_nodes_in_segment(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    {ok, Pid3} = cets:start(make_name(Config, 3), #{}),
    %% Pid2 and Pid3 are connected
    %% But for some reason Pid3 has a different join_ref
    %% (probably could happen if it still haven't checked other nodes after a join)
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3, #{}),
    set_join_ref(Pid3, make_ref()),
    {error, check_same_join_ref_failed} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}).

join_fails_because_pids_do_not_match_for_nodes_in_segment(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    {ok, Pid3} = cets:start(make_name(Config, 3), #{}),
    %% Pid2 and Pid3 are connected
    %% But for some reason Pid3 has a different other_nodes list
    %% (probably could happen if it still haven't checked other nodes after a join)
    ok = cets_join:join(lock_name(Config), #{}, Pid2, Pid3, #{}),
    set_other_servers(Pid3, []),
    {error, check_fully_connected_failed} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}).

join_fails_because_servers_overlap(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    {ok, Pid3} = cets:start(make_name(Config, 3), #{}),
    set_other_servers(Pid1, [Pid3]),
    set_other_servers(Pid2, [Pid3]),
    {error, check_do_not_overlap_failed} =
        cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}).

remote_ops_are_ignored_if_join_ref_does_not_match(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
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
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
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

send_dump_contains_already_added_servers(Config) ->
    %% Check that even if we have already added server in send_dump, nothing crashes
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    ok = cets_join:join(lock_name(Config), #{}, Pid1, Pid2, #{}),
    PauseRef = cets:pause(Pid1),
    %% That should be called by cets_join module
    cets:send_dump(Pid1, [Pid2], make_ref(), [{1}]),
    cets:unpause(Pid1, PauseRef),
    {ok, [{1}]} = cets:remote_dump(Pid1).

test_multinode(Config) ->
    Node1 = node(),
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab1,
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    {ok, Pid4} = start(Node4, Tab),
    ok = join(Node1, Tab, Pid1, Pid3),
    ok = join(Node2, Tab, Pid2, Pid4),
    insert(Node1, Tab, {a}),
    insert(Node2, Tab, {b}),
    insert(Node3, Tab, {c}),
    insert(Node4, Tab, {d}),
    [{a}, {c}] = dump(Node1, Tab),
    [{b}, {d}] = dump(Node2, Tab),
    ok = join(Node1, Tab, Pid2, Pid1),
    [{a}, {b}, {c}, {d}] = dump(Node1, Tab),
    [{a}, {b}, {c}, {d}] = dump(Node2, Tab),
    insert(Node1, Tab, {f}),
    insert(Node4, Tab, {e}),
    Same = fun(X) ->
        X = dump(Node1, Tab),
        X = dump(Node2, Tab),
        X = dump(Node3, Tab),
        X = dump(Node4, Tab),
        ok
    end,
    Same([{a}, {b}, {c}, {d}, {e}, {f}]),
    delete(Node1, Tab, e),
    Same([{a}, {b}, {c}, {d}, {f}]),
    delete(Node4, Tab, a),
    Same([{b}, {c}, {d}, {f}]),
    %% Bulk operations are supported
    insert_many(Node4, Tab, [{m}, {a}, {n}, {y}]),
    Same([{a}, {b}, {c}, {d}, {f}, {m}, {n}, {y}]),
    delete_many(Node4, Tab, [a, n]),
    Same([{b}, {c}, {d}, {f}, {m}, {y}]),
    ok.

test_multinode_remote_insert(Config) ->
    Tab = rem_tab,
    [Node2, Node3 | _] = proplists:get_value(nodes, Config),
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
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab3,
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
    [Node2, _Node3, _Node4] = proplists:get_value(nodes, Config),
    Tab = tab2,
    {ok, _Pid1} = start(Node1, Tab),
    {ok, _Pid2} = start(Node2, Tab),
    Dir = proplists:get_value(priv_dir, Config),
    ct:pal("Dir ~p", [Dir]),
    FileName = filename:join(Dir, "disco.txt"),
    ok = file:write_file(FileName, io_lib:format("~s~n~s~n", [Node1, Node2])),
    {ok, Disco} = cets_discovery:start(#{tables => [Tab], disco_file => FileName}),
    %% Waits for the first check
    sys:get_state(Disco),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := tab2}] =
        cets_discovery:info(Disco),
    ok.

test_locally(_Config) ->
    {ok, Pid1} = cets:start(t1, #{}),
    {ok, Pid2} = cets:start(t2, #{}),
    ok = cets_join:join(lock1, #{table => [t1, t2]}, Pid1, Pid2),
    cets:insert(t1, {1}),
    cets:insert(t1, {1}),
    cets:insert(t2, {2}),
    D = cets:dump(t1),
    D = cets:dump(t2).

handle_down_is_called(_Config) ->
    Parent = self(),
    DownFn = fun(#{remote_pid := _RemotePid, table := _Tab}) ->
        Parent ! down_called
    end,
    {ok, Pid1} = cets:start(d1, #{handle_down => DownFn}),
    {ok, Pid2} = cets:start(d2, #{}),
    ok = cets_join:join(lock1, #{table => [d1, d2]}, Pid1, Pid2),
    exit(Pid2, oops),
    receive
        down_called -> ok
    after 5000 -> ct:fail(timeout)
    end.

events_are_applied_in_the_correct_order_after_unpause(_Config) ->
    T = t4,
    {ok, Pid} = cets:start(T, #{}),
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

pause_multiple_times(_Config) ->
    T = t5,
    {ok, Pid} = cets:start(T, #{}),
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

unpause_twice(_Config) ->
    T = t6,
    {ok, Pid} = cets:start(T, #{}),
    PauseMon = cets:pause(Pid),
    ok = cets:unpause(Pid, PauseMon),
    {error, unknown_pause_monitor} = cets:unpause(Pid, PauseMon).

unpause_if_pause_owner_crashes(_Config) ->
    Me = self(),
    T = pause_crashed,
    {ok, Pid} = cets:start(T, #{}),
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

write_returns_if_remote_server_crashes(_Config) ->
    {ok, Pid1} = cets:start(c1, #{}),
    {ok, Pid2} = cets:start(c2, #{}),
    ok = cets_join:join(lock1, #{table => [c1, c2]}, Pid1, Pid2),
    sys:suspend(Pid2),
    R = cets:insert_request(c1, {1}),
    exit(Pid2, oops),
    {reply, ok} = cets:wait_response(R, 5000).

ack_process_stops_correctly(_Config) ->
    {ok, Pid} = cets:start(ack_stops, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    AckMon = monitor(process, AckPid),
    cets:stop(Pid),
    receive
        {'DOWN', AckMon, process, AckPid, normal} -> ok
    after 5000 -> ct:fail(timeout)
    end.

ack_process_handles_unknown_remote_server(_Config) ->
    {ok, Pid1} = cets:start(ack_unkn1, #{}),
    {ok, Pid2} = cets:start(ack_unkn2, #{}),
    ok = cets_join:join(lock_ack, #{}, Pid1, Pid2),
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

ack_process_handles_unknown_from(_Config) ->
    {ok, Pid1} = cets:start(ack_unkn_from1, #{}),
    {ok, Pid2} = cets:start(ack_unkn_from2, #{}),
    ok = cets_join:join(lock_ack, #{}, Pid1, Pid2),
    #{ack_pid := AckPid} = cets:info(Pid1),
    R = cets:insert_request(Pid1, {1}),
    From = {self(), make_ref()},
    cets_ack:ack(AckPid, From, self()),
    %% Ack process still works fine
    {reply, ok} = cets:wait_response(R, 5000).

ack_calling_add_when_server_list_is_empty_is_not_allowed(_Config) ->
    {ok, Pid} = cets:start(add_fails, #{}),
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

sync_using_name_works(_Config) ->
    {ok, _Pid1} = cets:start(c4, #{}),
    cets:sync(c4).

insert_many_request(_Config) ->
    {ok, Pid} = cets:start(c5, #{}),
    R = cets:insert_many_request(Pid, [{a}, {b}]),
    {reply, ok} = cets:wait_response(R, 5000),
    [{a}, {b}] = ets:tab2list(c5).

insert_into_bag(_Config) ->
    T = b1,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert(T, {1, 1}),
    cets:insert(T, {1, 1}),
    cets:insert(T, {1, 2}),
    [{1, 1}, {1, 2}] = lists:sort(cets:dump(T)).

delete_from_bag(_Config) ->
    T = b2,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}]),
    cets:delete_object(T, {1, 2}),
    [{1, 1}] = cets:dump(T).

delete_many_from_bag(_Config) ->
    T = b3,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}, {1, 3}, {1, 5}, {2, 3}]),
    cets:delete_objects(T, [{1, 2}, {1, 5}, {1, 4}]),
    [{1, 1}, {1, 3}, {2, 3}] = lists:sort(cets:dump(T)).

delete_request_from_bag(_Config) ->
    T = b4,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}]),
    R = cets:delete_object_request(T, {1, 2}),
    {reply, ok} = cets:wait_response(R, 5000),
    [{1, 1}] = cets:dump(T).

delete_request_many_from_bag(_Config) ->
    T = b5,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}, {1, 3}]),
    R = cets:delete_objects_request(T, [{1, 1}, {1, 3}]),
    {reply, ok} = cets:wait_response(R, 5000),
    [{1, 2}] = cets:dump(T).

insert_into_bag_is_replicated(_Config) ->
    {ok, Pid1} = cets:start(b6a, #{type => bag}),
    {ok, Pid2} = cets:start(T2 = b6b, #{type => bag}),
    ok = cets_join:join(join_lock_b6, #{}, Pid1, Pid2),
    cets:insert(Pid1, {1, 1}),
    [{1, 1}] = cets:dump(T2).

insert_into_keypos_table(_Config) ->
    T = kp1,
    {ok, _Pid} = cets:start(T, #{keypos => 2}),
    cets:insert(T, {rec, 1}),
    cets:insert(T, {rec, 2}),
    [{rec, 1}] = lists:sort(ets:lookup(T, 1)),
    [{rec, 1}, {rec, 2}] = lists:sort(cets:dump(T)).

table_name_works(_Config) ->
    T = tabnamecheck,
    {ok, Pid} = cets:start(T, #{}),
    {ok, T} = cets:table_name(T),
    {ok, T} = cets:table_name(Pid),
    #{table := T} = cets:info(Pid).

info_contains_opts(_Config) ->
    {ok, Pid} = cets:start(info_contains_opts, #{type => bag}),
    #{opts := #{type := bag}} = cets:info(Pid).

%% Cases to improve code coverage

unknown_down_message_is_ignored(_Config) ->
    {ok, Pid} = cets:start(rand_down_msg, #{}),
    RandPid = spawn(fun() -> ok end),
    Pid ! {'DOWN', make_ref(), process, RandPid, oops},
    still_works(Pid).

unknown_message_is_ignored(_Config) ->
    {ok, Pid} = cets:start(unkn_msg, #{}),
    Pid ! oops,
    still_works(Pid).

unknown_cast_message_is_ignored(_Config) ->
    {ok, Pid} = cets:start(unkn_cast_msg, #{}),
    gen_server:cast(Pid, oops),
    still_works(Pid).

unknown_message_is_ignored_in_ack_process(_Config) ->
    {ok, Pid} = cets:start(ack_unkn_msg, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    AckPid ! oops,
    still_works(Pid).

unknown_cast_message_is_ignored_in_ack_process(_Config) ->
    {ok, Pid} = cets:start(ack_unkn_cast_msg, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    gen_server:cast(AckPid, oops),
    still_works(Pid).

unknown_call_returns_error_from_ack_process(_Config) ->
    {ok, Pid} = cets:start(ack_unkn_call_msg, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    {error, unexpected_call} = gen_server:call(AckPid, oops),
    still_works(Pid).

code_change_returns_ok(_Config) ->
    {ok, Pid} = cets:start(ack_code_chg, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    sys:suspend(AckPid),
    ok = sys:change_code(AckPid, cets, v2, []),
    sys:resume(AckPid).

code_change_returns_ok_for_ack(_Config) ->
    {ok, Pid} = cets:start(code_chg, #{}),
    #{ack_pid := AckPid} = cets:info(Pid),
    sys:suspend(AckPid),
    ok = sys:change_code(AckPid, cets_ack, v2, []),
    sys:resume(AckPid).

run_spawn_forwards_errors(_Config) ->
    matched =
        try
            cets_long:run_spawn(#{}, fun() -> error(oops) end)
        catch
            error:oops ->
                matched
        end.

still_works(Pid) ->
    pong = cets:ping(Pid),
    %% The server works fine
    ok = cets:insert(Pid, {1}),
    {ok, [{1}]} = cets:remote_dump(Pid).

start(Node, Tab) ->
    rpc(Node, cets, start, [Tab, #{}]).

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

rpc(Node, M, F, Args) ->
    case rpc:call(Node, M, F, Args) of
        {badrpc, Error} ->
            ct:fail({badrpc, Error});
        Other ->
            Other
    end.

start_node(Sname) ->
    {ok, _Peer, Node} = peer:start(#{name => Sname}),
    ok = rpc:call(Node, code, add_paths, [code:get_path()]),
    Node.

receive_message(M) ->
    receive
        M -> ok
    after 5000 -> error({receive_message_timeout, M})
    end.

make_name(Config, Num) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ integer_to_list(Num)).

lock_name(Config) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_lock").

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
