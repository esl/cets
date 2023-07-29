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
        other_pids_call_work_after_join,
        inserted_records_could_be_read_back_from_replicated_table,
        join_works_with_existing_data,
        join_works_with_existing_data_with_conflicts,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_more_keys,
        join_works_with_existing_data_with_conflicts_and_defined_conflict_handler_and_keypos2,
        bag_with_conflict_handler_not_allowed,
        join_with_the_same_pid,
        join_start_fails,
        join_fails_before_apply_dump,
        join_fails_before_apply_dump_with_partial_apply,
        join_fails_then_pending_ops_are_filtered,
        join_fails_then_old_alias_is_disabled,
        apply_dump_with_unknown_dump_ref_would_be_ignored,
        test_multinode,
        node_list_is_correct,
        test_multinode_auto_discovery,
        test_locally,
        handle_down_is_called,
        events_are_applied_in_the_correct_order_after_unpause,
        pause_multiple_times,
        unpause_when_pause_owner_crashes,
        unpause_twice,
        write_returns_if_remote_server_crashes,
        mon_cleaner_works,
        mon_cleaner_stops_correctly,
        sync_using_name_works,
        insert_many_request,
        insert_into_bag,
        delete_from_bag,
        delete_many_from_bag,
        delete_request_from_bag,
        delete_request_many_from_bag,
        insert_into_bag_is_replicated,
        insert_into_keypos_table,
        info_contains_opts,
        updated_is_not_received_after_timeout,
        remote_down_is_not_received_after_timeout
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

other_pids_call_work_after_join(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2),
    [Pid2] = cets:other_pids(Pid1),
    [Pid1] = cets:other_pids(Pid2).

inserted_records_could_be_read_back_from_replicated_table(_Config) ->
    {ok, Pid1} = cets:start(ins1tab, #{}),
    {ok, Pid2} = cets:start(ins2tab, #{}),
    ok = cets_join:join(join_lock1_ins, #{}, Pid1, Pid2),
    cets:insert(ins1tab, {alice, 32}),
    [{alice, 32}] = ets:lookup(ins2tab, alice).

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
    {error, same_pid} = cets_join:join(joinsame_lock1_con, #{}, Pid, Pid),
    Nodes = [node()],
    %% The process is still running and no data loss (i.e. size is not zero)
    #{nodes := Nodes, size := 1} = cets:info(Pid).

join_start_fails(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    F = fun
        (join_start) -> error(sim_error);
        (_) -> ok
    end,
    {error, {error, sim_error, _}} =
        cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    [] = cets:other_pids(Pid1),
    [] = cets:other_pids(Pid2).

join_fails_before_apply_dump(Config) ->
    Me = self(),
    DownFn = fun(#{remote_pid := RemotePid, table := _Tab}) ->
        Me ! {down_called, self(), RemotePid}
    end,
    {ok, Pid1} = cets:start(make_name(Config, 1), #{handle_down => DownFn}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    cets:insert(Pid1, {1}),
    cets:insert(Pid2, {2}),
    ExpectedAllPids = [Pid1, Pid2],
    F = fun
        ({all_pids_known, Pids}) ->
            Pids = ExpectedAllPids,
            Me ! all_pids_known;
        ({before_apply_dump, 1, P}) when Pid2 =:= P ->
            error(sim_error);
        (_) ->
            ok
    end,
    {error, {error, sim_error, _}} =
        cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    receive_message(all_pids_known),
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

join_fails_before_apply_dump_with_partial_apply(Config) ->
    Me = self(),
    [Pid1, Pid2, Pid3, Pid4] = make_n_servers(4, Config),
    %% Pid1, Pid3, Pid4 are in the one network segment
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid3, #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid4, #{}),
    [Pid3, Pid4] = cets:other_pids(Pid1),
    cets:insert(Pid1, {1}),
    cets:insert(Pid2, {2}),
    ExpectedAllPids = [Pid1, Pid3, Pid4, Pid2],
    F = fun
        ({all_pids_known, Pids}) ->
            Pids = ExpectedAllPids,
            Me ! all_pids_known;
        ({before_apply_dump, 2, P}) when Pid4 =:= P ->
            error(sim_error);
        (_) ->
            ok
    end,
    {error, {error, sim_error, _}} =
        cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    receive_message(all_pids_known),
    %% Not joined fully, some data exchanged
    [cets:sync(P) || P <- ExpectedAllPids],
    %% Bad join disconnects Pid4 from the old connections
    [Pid3] = cets:other_pids(Pid1),
    [] = cets:other_pids(Pid2),
    [Pid1] = cets:other_pids(Pid3),
    [] = cets:other_pids(Pid4),
    {ok, [{1}, {2}]} = cets:remote_dump(Pid1),
    {ok, [{2}]} = cets:remote_dump(Pid2),
    {ok, [{1}, {2}]} = cets:remote_dump(Pid3),
    {ok, [{1}]} = cets:remote_dump(Pid4).

join_fails_then_pending_ops_are_filtered(Config) ->
    Me = self(),
    [Pid1, Pid2, Pid3, Pid4] = make_n_servers(4, Config),
    %% Pid1, Pid3, Pid4 are in the one network segment
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid3, #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid4, #{}),
    [Pid3, Pid4] = cets:other_pids(Pid1),
    ExpectedAllPids = [Pid1, Pid3, Pid4, Pid2],
    F = fun
        ({all_pids_known, Pids}) ->
            Pids = ExpectedAllPids,
            Me ! all_pids_known;
        ({before_apply_dump, 2, P}) when Pid4 =:= P ->
            error(sim_error);
        (paused) ->
            %% Add some pending ops and check if they would be replicated later
            cets:insert_request(Pid1, {p1}),
            cets:insert_request(Pid2, {p2}),
            cets:insert_request(Pid3, {p3}),
            cets:insert_request(Pid4, {p4});
        (_) ->
            ok
    end,
    {error, {error, sim_error, _}} =
        cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    receive_message(all_pids_known),
    %% Not joined fully, some data exchanged
    [cets:sync(P) || P <- ExpectedAllPids],
    {ok, [{p1}, {p3}]} = cets:remote_dump(Pid1),
    {ok, [{p2}]} = cets:remote_dump(Pid2),
    {ok, [{p1}, {p3}]} = cets:remote_dump(Pid3),
    {ok, [{p4}]} = cets:remote_dump(Pid4).

join_fails_then_old_alias_is_disabled(Config) ->
    Me = self(),
    [Pid1, Pid2, Pid3, Pid4] = make_n_servers(4, Config),
    %% Pid1, Pid3, Pid4 are in the one network segment
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid3, #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid4, #{}),
    #{server_to_dest := #{Pid1 := Dest3to1}} = cets:info(Pid3),
    #{server_to_dest := #{Pid1 := Dest4to1}} = cets:info(Pid4),
    [Pid3, Pid4] = cets:other_pids(Pid1),
    ExpectedAllPids = [Pid1, Pid3, Pid4, Pid2],
    F = fun
        ({all_pids_known, Pids}) ->
            Pids = ExpectedAllPids,
            Me ! all_pids_known;
        ({before_apply_dump, 2, P}) when Pid4 =:= P ->
            error(sim_error);
        (_) ->
            ok
    end,
    {error, {error, sim_error, _}} =
        cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    receive_message(all_pids_known),
    %% Not joined fully, some data exchanged
    [cets:sync(P) || P <- ExpectedAllPids],
    %% Simulate remote_op-s
    %% Dest4to1 alias should be deactivated
    %% Dest3to1 alias should work
    Dest3to1 ! {remote_op, Dest3to1, make_ref(), {insert, {z3}}},
    Dest4to1 ! {remote_op, Dest4to1, make_ref(), {insert, {z4}}},
    %% Ensure remote_op-s are received
    cets:ping(Pid1),
    {ok, [{z3}]} = cets:remote_dump(Pid1).

apply_dump_with_unknown_dump_ref_would_be_ignored(Config) ->
    Me = self(),
    [Pid1, Pid2] = make_n_servers(2, Config),
    F = fun
        ({before_apply_dump, 0, Pid}) ->
            {error, unknown_dump_ref} = cets:apply_dump(Pid, make_ref()),
            Me ! before_apply_dump_called;
        (_) ->
            ok
    end,
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2, #{step_handler => F}),
    receive_message(before_apply_dump_called),
    %% Check that join is successful
    ok = cets:insert(Pid1, {1}),
    {ok, [{1}]} = cets:remote_dump(Pid2).

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
    [ok = cets:wait_response(R, 5000) || R <- [R1, R2, R3, R4]],
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
    cets:wait_response(Ref1, 5000),
    cets:wait_response(Ref2, 5000),
    [{1}, {2}] = lists:sort(cets:dump(T)).

unpause_when_pause_owner_crashes(Config) ->
    Me = self(),
    {ok, Pid} = cets:start(make_name(Config, 1), #{}),
    Other = spawn(fun() ->
        cets:pause(Pid),
        Me ! paused,
        receive
            ok -> ok
        end
    end),
    Ref = cets:insert_request(Pid, {1}),
    receive_message(paused),
    erlang:exit(Other, crash_please),
    ok = cets:wait_response(Ref, 5000).

unpause_twice(_Config) ->
    T = t6,
    {ok, Pid} = cets:start(T, #{}),
    PauseMon = cets:pause(Pid),
    ok = cets:unpause(Pid, PauseMon),
    {error, unknown_pause_monitor} = cets:unpause(Pid, PauseMon).

write_returns_if_remote_server_crashes(_Config) ->
    {ok, Pid1} = cets:start(c1, #{}),
    {ok, Pid2} = cets:start(c2, #{}),
    ok = cets_join:join(lock1, #{table => [c1, c2]}, Pid1, Pid2),
    sys:suspend(Pid2),
    R = cets:insert_request(c1, {1}),
    exit(Pid2, oops),
    ok = cets:wait_response(R, 5000).

mon_cleaner_works(_Config) ->
    {ok, Pid1} = cets:start(c3, #{}),
    %% Two cases to check: an alive process and a dead process
    R = cets:insert_request(c3, {2}),
    %% Ensure insert_request reaches the server
    cets:ping(Pid1),
    %% There is one monitor
    1 = maps:size(gen_server:call(c3_mon, dump)),
    {Pid, Mon} = spawn_monitor(fun() -> cets:insert_request(c3, {1}) end),
    receive
        {'DOWN', Mon, process, Pid, _Reason} -> ok
    after 5000 -> ct:fail(timeout)
    end,
    %% Ensure insert_request reaches the server
    cets:ping(Pid1),
    %% There are two monitors
    2 = maps:size(gen_server:call(c3_mon, dump)),
    %% Force check
    c3_mon ! check,
    %% A monitor for a dead process is removed
    1 = maps:size(gen_server:call(c3_mon, dump)),
    %% The monitor is finally removed once wait_response returns
    ok = cets:wait_response(R, 5000),
    0 = maps:size(gen_server:call(c3_mon, dump)).

mon_cleaner_stops_correctly(_Config) ->
    {ok, Pid} = cets:start(cleaner_stops, #{}),
    #{mon_pid := MonPid} = cets:info(Pid),
    MonMon = monitor(process, MonPid),
    cets:stop(Pid),
    receive
        {'DOWN', MonMon, process, MonPid, normal} -> ok
    after 5000 -> ct:fail(timeout)
    end.

sync_using_name_works(_Config) ->
    {ok, _Pid1} = cets:start(c4, #{}),
    cets:sync(c4).

insert_many_request(_Config) ->
    {ok, Pid} = cets:start(c5, #{}),
    R = cets:insert_many_request(Pid, [{a}, {b}]),
    ok = cets:wait_response(R, 5000),
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
    ok = cets:wait_response(R, 5000),
    [{1, 1}] = cets:dump(T).

delete_request_many_from_bag(_Config) ->
    T = b5,
    {ok, _Pid} = cets:start(T, #{type => bag}),
    cets:insert_many(T, [{1, 1}, {1, 2}, {1, 3}]),
    R = cets:delete_objects_request(T, [{1, 1}, {1, 3}]),
    ok = cets:wait_response(R, 5000),
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

info_contains_opts(_Config) ->
    {ok, Pid} = cets:start(info_contains_opts, #{type => bag}),
    #{opts := #{type := bag}} = cets:info(Pid).

updated_is_not_received_after_timeout(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2),
    sys:suspend(Pid2),
    R = cets:insert_request(Pid1, {1}),
    wait_response_fails_with_timeout(R),
    sys:resume(Pid2),
    %% Ensure that cets_updated message reaches us and filtered out
    cets:ping(Pid2),
    ensure_no_updated_message().

remote_down_is_not_received_after_timeout(Config) ->
    {ok, Pid1} = cets:start(make_name(Config, 1), #{}),
    {ok, Pid2} = cets:start(make_name(Config, 2), #{}),
    ok = cets_join:join(make_name(Config, 0), #{}, Pid1, Pid2),
    sys:suspend(Pid2),
    R = cets:insert_request(Pid1, {1}),
    wait_response_fails_with_timeout(R),
    Ref = erlang:monitor(process, Pid2),
    exit(Pid2, kill),
    receive_down_for_monitor(Ref),
    cets:ping(Pid1),
    %% The main reason we don't receive the message is that
    %% we've demonitored after wait_response failed.
    %% And cets_mon_cleaner would try to respond to our invalidated alias.
    %% Though, cets_mon_cleaner would probably receive delete request for
    %% the alias before.
    ensure_no_down_message().

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
    {ok, Node} = ct_slave:start(Sname, [{monitor_master, true}]),
    rpc:call(Node, code, add_paths, [code:get_path()]),
    Node.

make_name(Config, Num) ->
    Testcase = proplists:get_value(testcase, Config),
    list_to_atom(atom_to_list(Testcase) ++ "_" ++ integer_to_list(Num)).

wait_response_fails_with_timeout(R) ->
    try
        cets:wait_response(R, 0),
        error(expected_timeout)
    catch
        error:timeout -> ok
    end.

ensure_no_updated_message() ->
    receive
        {cets_updated, _, _} ->
            error(unexpected_updated)
    after 0 -> ok
    end.

ensure_no_down_message() ->
    receive
        {cets_remote_down, _, _} ->
            error(unexpected_remote_down)
    after 0 -> ok
    end.

receive_down_for_monitor(Mon) ->
    receive
        {'DOWN', Mon, process, _Pid, _Reason} -> ok
    after 5000 -> ct:fail(timeout)
    end.

receive_message(M) ->
    receive
        M -> ok
    after 5000 -> error({receive_message_timeout, M})
    end.

make_n_servers(N, Config) ->
    lists:map(
        fun(X) ->
            {ok, Pid} = cets:start(make_name(Config, X), #{}),
            Pid
        end,
        lists:seq(1, N)
    ).
