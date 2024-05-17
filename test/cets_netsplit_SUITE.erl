-module(cets_netsplit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-compile([export_all, nowarn_export_all]).

-import(cets_test_setup, [
    start/2,
    make_name/1,
    make_name/2,
    lock_name/1
]).

-import(cets_test_setup, [
    given_two_joined_tables/1
]).

-import(cets_test_peer, [
    block_node/2,
    reconnect_node/2
]).

-import(cets_test_rpc, [
    rpc/4,
    dump/2
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
        cets_ping_non_existing_node,
        cets_ping_net_family
    ].

seq_cases() ->
    [
        insert_returns_when_netsplit,
        inserts_after_netsplit_reconnects,
        cets_ping_all_returns_when_ping_crashes,
        ping_pairs_returns_pongs,
        ping_pairs_returns_earlier,
        pre_connect_fails_on_our_node,
        pre_connect_fails_on_one_of_the_nodes
    ].

cets_seq_no_log_cases() ->
    [
        node_down_history_is_updated_when_netsplit_happens
    ].

init_per_suite(Config) ->
    cets_test_setup:init_cleanup_table(),
    cets_test_peer:start([ct2, ct3, ct5], Config).

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

cets_ping_all_returns_when_ping_crashes(Config) ->
    #{pid1 := Pid1, pid2 := Pid2} = given_two_joined_tables(Config),
    meck:new(cets, [passthrough]),
    meck:expect(cets_call, long_call, fun
        (Server, ping) when Server == Pid2 -> error(simulate_crash);
        (Server, Msg) -> meck:passthrough([Server, Msg])
    end),
    ?assertMatch({error, [{Pid2, {'EXIT', {simulate_crash, _}}}]}, cets:ping_all(Pid1)),
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
        F = fun() ->
            History = maps:get(node_down_history, cets:info(Pid1)),
            lists:map(fun(#{node := Node}) -> Node end, History)
        end,
        cets_test_wait:wait_until(F, [Node5])
    after
        reconnect_node(Node5, Peer5),
        cets:stop(Pid5)
    end.

cets_ping_non_existing_node(_Config) ->
    pang = cets_ping:ping('mongooseim@non_existing_host').

pre_connect_fails_on_our_node(_Config) ->
    cets_test_setup:mock_epmd(),
    %% We would fail to connect to the remote EPMD but we would get an IP
    pang = cets_ping:ping('mongooseim@resolvabletobadip'),
    meck:unload().

pre_connect_fails_on_one_of_the_nodes(Config) ->
    #{ct2 := Node2} = proplists:get_value(nodes, Config),
    cets_test_setup:mock_epmd(),
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
