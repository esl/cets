-module(cets_status_SUITE).
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
    wait_for_ready/2,
    wait_till_test_stage/2
]).

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

-import(cets_test_helper, [
    assert_unique/1,
    set_other_servers/2
]).

-import(cets_test_rpc, [
    other_nodes/2
]).

all() ->
    [
        {group, cets}
    ].

groups() ->
    %% Cases should have unique names, because we name CETS servers based on case names
    [
        {cets, [parallel, {repeat_until_any_fail, 3}], assert_unique(cases())}
    ].

cases() ->
    [
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
        format_data_does_not_return_table_duplicates
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
    ok = wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{unavailable_nodes := ['badnode@localhost']}, cets_status:status(DiscoName)).

status_unavailable_nodes_is_subset_of_discovery_nodes(Config) ->
    Node1 = node(),
    Self = self(),
    GetFn1 = fun(State) -> {{ok, [Node1, 'badnode@localhost']}, State} end,
    GetFn2 = fun(State) ->
        Self ! get_fn2_called,
        {{ok, [Node1]}, State}
    end,
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
    ok = wait_for_ready(DiscoName, 5000),
    ?assertMatch(#{unavailable_nodes := ['badnode@localhost']}, cets_status:status(DiscoName)),
    %% Remove badnode from disco
    meck:expect(BackendModule, get_nodes, GetFn2),
    %% Force check.
    Disco ! check,
    receive_message(get_fn2_called),
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
    ok = wait_for_ready(DiscoName, 5000),
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
    ok = wait_for_ready(DiscoName, 5000),
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
    ok = wait_for_ready(DiscoName, 5000),
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
    ok = wait_for_ready(DiscoName, 5000),
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
    ok = wait_for_ready(DiscoName, 5000),
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
    ok = wait_for_ready(DiscoName, 5000),
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

    ok = wait_for_ready(DiscoName, 5000),
    set_other_servers(Pid22, []),
    cets_test_wait:wait_until(
        fun() -> maps:get(conflict_nodes, cets_status:status(DiscoName)) end, [Node2]
    ),
    cets_test_wait:wait_until(
        fun() -> maps:get(conflict_tables, cets_status:status(DiscoName)) end, [Tab2]
    ).

format_data_does_not_return_table_duplicates(Config) ->
    Res = cets_status:format_data(test_data_for_duplicate_missing_table_in_status(Config)),
    ?assertMatch(#{remote_unknown_tables := [], remote_nodes_with_missing_tables := []}, Res).

%% Helpers

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
