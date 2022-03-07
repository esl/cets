-module(kiss_SUITE).
-include_lib("common_test/include/ct.hrl").
 
-compile([export_all, nowarn_export_all]).
 
all() -> [test_multinode, node_list_is_correct,
          test_multinode_auto_discovery, test_locally,
          handle_down_is_called,
          events_are_applied_in_the_correct_order_after_unpause,
          write_returns_if_remote_server_crashes,
          mon_cleaner_works, sync_using_name_works].
 
init_per_suite(Config) ->
    Node2 = start_node(ct2),
    Node3 = start_node(ct3),
    Node4 = start_node(ct4),
    [{nodes, [Node2, Node3, Node4]}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(test_multinode_auto_discovery, Config) ->
    ct:make_priv_dir(),
    Config;
init_per_testcase(_, Config) ->
    Config.
 
end_per_testcase(_, _Config) ->
    ok.
 
test_multinode(Config) ->
    Node1 = node(),
    [Node2, Node3, Node4] = proplists:get_value(nodes, Config),
    Tab = tab1,
    {ok, Pid1} = start(Node1, Tab),
    {ok, Pid2} = start(Node2, Tab),
    {ok, Pid3} = start(Node3, Tab),
    {ok, Pid4} = start(Node4, Tab),
    ok = join(Node1, Tab, Pid3, Pid1),
    ok = join(Node2, Tab, Pid4, Pid2),
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
               X = dump(Node4, Tab)
           end,
    Same([{a}, {b}, {c}, {d}, {e}, {f}]),
    delete(Node1, Tab, e),
    Same([{a}, {b}, {c}, {d}, {f}]),
    delete(Node4, Tab, a),
    Same([{b}, {c}, {d}, {f}]),
    %% Bulk operations are supported
    insert(Node4, Tab, [{m}, {a}, {n}, {y}]),
    Same([{a}, {b}, {c}, {d}, {f}, {m}, {n}, {y}]),
    delete_many(Node4, Tab, [a,n]),
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
    ok = join(Node1, Tab, Pid3, Pid1),
    ok = join(Node2, Tab, Pid4, Pid2),
    ok = join(Node1, Tab, Pid2, Pid1),
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
    {ok, Disco} = kiss_discovery:start(#{tables => [Tab], disco_file => FileName}),
    %% Waits for the first check
    sys:get_state(Disco),
    [Node2] = other_nodes(Node1, Tab),
    [#{memory := _, nodes := [Node1, Node2], size := 0, table := tab2}]
        = kiss_discovery:info(Disco),
    ok.

test_locally(_Config) ->
    {ok, Pid1} = kiss:start(t1, #{}),
    {ok, Pid2} = kiss:start(t2, #{}),
    ok = kiss_join:join(lock1, #{table => [t1, t2]}, Pid1, Pid2),
    kiss:insert(t1, {1}),
    kiss:insert(t1, {1}),
    kiss:insert(t2, {2}),
    D = kiss:dump(t1),
    D = kiss:dump(t2).

handle_down_is_called(_Config) ->
    Parent = self(),
    DownFn = fun(#{remote_pid := _RemotePid, table := _Tab}) ->
                     Parent ! down_called
             end,
    {ok, Pid1} = kiss:start(d1, #{handle_down => DownFn}),
    {ok, Pid2} = kiss:start(d2, #{}),
    ok = kiss_join:join(lock1, #{table => [d1, d2]}, Pid1, Pid2),
    exit(Pid2, oops),
    receive
        down_called -> ok
    after 5000 -> ct:fail(timeout)
    end.

events_are_applied_in_the_correct_order_after_unpause(_Config) ->
    T = t4,
    {ok, Pid} = kiss:start(T, #{}),
    ok = kiss:pause(Pid),
    R1 = kiss:insert_request(T, {1}),
    R2 = kiss:delete_request(T, 1),
    kiss:delete_request(T, 2),
    kiss:insert_request(T, {2}),
    kiss:insert_request(T, {3}),
    kiss:insert_request(T, {4}),
    kiss:insert_request(T, {5}),
    R3 = kiss:insert_request(T, [{6}, {7}]),
    R4 = kiss:delete_many_request(T, [5, 4]),
    [] = lists:sort(kiss:dump(T)),
    ok = kiss:unpause(Pid),
    [ok = kiss:wait_response(R, 5000) || R <- [R1, R2, R3, R4]],
    [{2}, {3}, {6}, {7}] = lists:sort(kiss:dump(T)).

write_returns_if_remote_server_crashes(_Config) ->
    {ok, Pid1} = kiss:start(c1, #{}),
    {ok, Pid2} = kiss:start(c2, #{}),
    ok = kiss_join:join(lock1, #{table => [c1, c2]}, Pid1, Pid2),
    sys:suspend(Pid2),
    R = kiss:insert_request(c1, {1}),
    exit(Pid2, oops),
    ok = kiss:wait_response(R, 5000).

mon_cleaner_works(_Config) ->
    {ok, Pid1} = kiss:start(c3, #{}),
    %% Suspend, so to avoid unexpected check
    sys:suspend(c3_mon),
    %% Two cases to check: an alive process and a dead process 
    R = kiss:insert_request(c3, {2}),
    %% Ensure insert_request reaches the server
    kiss:ping(Pid1),
    %% There is one monitor
    [_] = ets:tab2list(c3_mon),
    {Pid, Mon} = spawn_monitor(fun() -> kiss:insert_request(c3, {1}) end),
    receive
        {'DOWN', Mon, process, Pid, _Reason} -> ok
    after 5000 -> ct:fail(timeout)
    end,
    %% Ensure insert_request reaches the server
    kiss:ping(Pid1),
    %% There are two monitors
    [_, _] = ets:tab2list(c3_mon),
    %% Force check
    sys:resume(c3_mon),
    c3_mon ! check,
    %% Ensure, that check is finished
    sys:get_state(c3_mon),
    %% A monitor for a dead process is removed
    [_] = ets:tab2list(c3_mon),
    %% The monitor is finally removed once wait_response returns
    ok = kiss:wait_response(R, 5000),
    [] = ets:tab2list(c3_mon).

sync_using_name_works(_Config) ->
    {ok, _Pid1} = kiss:start(c4, #{}),
    kiss:sync(c4).

start(Node, Tab) ->
    rpc(Node, kiss, start, [Tab, #{}]).

insert(Node, Tab, Rec) ->
    rpc(Node, kiss, insert, [Tab, Rec]).

delete(Node, Tab, Key) ->
    rpc(Node, kiss, delete, [Tab, Key]).

delete_many(Node, Tab, Keys) ->
    rpc(Node, kiss, delete_many, [Tab, Keys]).

dump(Node, Tab) ->
    rpc(Node, kiss, dump, [Tab]).

other_nodes(Node, Tab) ->
    rpc(Node, kiss, other_nodes, [Tab]).

join(Node1, Tab, Pid1, Pid2) ->
    rpc(Node1, kiss_join, join, [lock1, #{table => Tab}, Pid1, Pid2]).

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
