-module(cets_status).
-export([status/1]).
-ignore_xref([status/1]).
-include_lib("kernel/include/logger.hrl").

-type tab_nodes() :: {Table :: atom(), Nodes :: ordsets:ordset(node())}.
-type table_name() :: cets:table_name().
-type disco_name() :: atom().

-type info() :: #{
    %% Nodes that are connected to us and have the CETS disco process started.
    available_nodes := [node()],
    %% Nodes that do not respond to our pings.
    unavailable_nodes := [node()],
    %% Nodes with stopped disco
    remote_nodes_without_disco := [node()],
    %% Nodes that has our local tables running (but could also have some unknown tables).
    %% All joined nodes replicate data between each other.
    joined_nodes := [node()],
    %% Nodes that have more tables registered than the local node.
    remote_nodes_with_unknown_tables := [node()],
    remote_unknown_tables := [table_name()],
    %% Nodes that are available, but do not host one of our local tables.
    remote_nodes_with_missing_tables => [node()],
    remote_missing_tables := [table_name()],
    %% Nodes that replicate at least one of our local tables to a different list of nodes
    %% (could temporary happen during a netsplit)
    conflict_nodes := [node()],
    conflict_tables := [table_name()],
    %% Nodes that are extracted from the discovery backend.
    discovered_nodes := [node()],
    %% True, if discovery backend returned the list of nodes the last time we've tried
    %% to call it.
    discovery_works := boolean()
}.

-spec status(disco_name()) -> info().
status(Disco) when is_atom(Disco) ->
    %% The node lists could not match for different nodes
    %% because they are updated periodically
    #{unavailable_nodes := UnNodes, nodes := DiscoNodes, tables := Tables} =
        Info = cets_discovery:system_info(Disco),
    DiscoNodesSorted = lists:sort(DiscoNodes),
    OnlineNodes = [node() | nodes()],
    AvailNodes = available_nodes(Disco, OnlineNodes),
    NoDiscoNodes = remote_nodes_without_disco(DiscoNodesSorted, AvailNodes, OnlineNodes),
    {Expected, OtherTabNodes} = gather_tables_and_replication_nodes(AvailNodes, Tables, Disco),
    JoinedNodes = joined_nodes(Expected, OtherTabNodes),
    {ConflictTables, ConflictNodes} = conflict_tables(Expected, OtherTabNodes),
    AllTables = all_tables(Expected, OtherTabNodes),
    {UnknownTables, NodesWithUnknownTables} = unknown_tables(OtherTabNodes, Tables, AllTables),
    #{
        unavailable_nodes => UnNodes,
        available_nodes => AvailNodes,
        remote_nodes_without_disco => NoDiscoNodes,
        joined_nodes => JoinedNodes,
        remote_nodes_with_unknown_tables => NodesWithUnknownTables,
        remote_unknown_tables => UnknownTables,
        conflict_nodes => ConflictNodes,
        conflict_tables => ConflictTables,
        discovered_nodes => DiscoNodesSorted,
        discovery_works => discovery_works(Info)
    }.

%% Nodes, that host the discovery process
-spec available_nodes(disco_name(), [node()]) -> [node()].
available_nodes(Disco, OnlineNodes) ->
    [Node || Node <- OnlineNodes, is_disco_running_on(Node, Disco)].

remote_nodes_without_disco(DiscoNodes, AvailNodes, OnlineNodes) ->
    [
        Node
     || Node <- DiscoNodes, lists:member(Node, OnlineNodes), not lists:member(Node, AvailNodes)
    ].

-spec is_disco_running_on(node(), disco_name()) -> boolean().
is_disco_running_on(Node, Disco) ->
    is_pid(rpc:call(Node, erlang, whereis, [Disco])).

gather_tables_and_replication_nodes(AvailNodes, Tables, Disco) ->
    OtherNodes = lists:delete(node(), AvailNodes),
    Expected = node_list_for_tables(node(), Tables),
    OtherTabNodes = [{Node, node_list_for_tables_from_disco(Node, Disco)} || Node <- OtherNodes],
    {Expected, maps:from_list(OtherTabNodes)}.

%% Nodes that has our local tables running (but could also have some unknown tables).
%% All joined nodes replicate data between each other.
joined_nodes(Expected, OtherTabNodes) ->
    ExpectedTables = maps:keys(Expected),
    OtherJoined = [
        Node
     || {Node, NodeTabs} <- maps:to_list(OtherTabNodes),
        maps:with(ExpectedTables, NodeTabs) =:= Expected
    ],
    lists:sort([node() | OtherJoined]).

unknown_tables(OtherTabNodes, Tables, AllTables) ->
    UnknownTables = ordsets:subtract(AllTables, Tables),
    NodesWithUnknownTables = [
        Node
     || {Node, TabNodes} <- maps:to_list(OtherTabNodes),
        tabnodes_has_any_of(TabNodes, UnknownTables)
    ],
    {UnknownTables, NodesWithUnknownTables}.

tabnodes_has_any_of(TabNodes, UnknownTables) ->
    lists:any(fun(Tab) -> maps:is_key(Tab, TabNodes) end, UnknownTables).

%% Nodes that replicate at least one of our local tables to a different list of nodes
%% (could temporary happen during a netsplit)
-spec conflict_tables([tab_nodes()], [{node(), [tab_nodes()]}]) -> {[table_name()], [table_name()]}.
conflict_tables(Expected, OtherTabNodes) ->
    F = fun(Node, NodeTabs, Acc) ->
        FF = fun(Table, OtherNodes, Acc2) ->
            case maps:get(Table, Expected, undefined) of
                Nodes when Nodes =:= OtherNodes ->
                    Acc2;
                undefined ->
                    Acc2;
                _ ->
                    [{Table, Node} | Acc2]
            end
        end,
        maps:fold(FF, Acc, NodeTabs)
    end,
    TabNodes = maps:fold(F, [], OtherTabNodes),
    {ConflictTables, ConflictNodes} = lists:unzip(TabNodes),
    {lists:usort(ConflictTables), lists:usort(ConflictNodes)}.

-spec all_tables([tab_nodes()], [{node(), [tab_nodes()]}]) -> [table_name()].
all_tables(Expected, OtherTabNodes) ->
    TableNodesVariants = [Expected | maps:values(OtherTabNodes)],
    TableVariants = lists:map(fun maps:keys/1, TableNodesVariants),
    %   SharedTables = ordsets:intersection(TableVariants),
    ordsets:union(TableVariants).

%% Returns nodes for each table hosted on node()
-spec node_list_for_tables_from_disco(node(), disco_name()) -> map().
node_list_for_tables_from_disco(Node, Disco) ->
    Tables = get_tables_list_on_node(Node, Disco),
    node_list_for_tables(Node, Tables).

%% Returns nodes for each table in the Tables list
-spec node_list_for_tables(node(), [table_name()]) -> map().
node_list_for_tables(Node, Tables) ->
    maps:from_list([{Table, node_list_for_table(Node, Table)} || Table <- Tables]).

-spec get_tables_list_on_node(Node :: node(), Disco :: disco_name()) -> [table_name()].
get_tables_list_on_node(Node, Disco) ->
    case rpc:call(Node, cets_discovery, get_tables, [Disco]) of
        {ok, Tables} ->
            Tables;
        _ ->
            []
    end.

-spec node_list_for_table(Node :: node(), Table :: table_name()) ->
    Nodes :: ordsets:ordset(node()).
node_list_for_table(Node, Table) ->
    case catch rpc:call(Node, cets, other_nodes, [Table]) of
        List when is_list(List) ->
            ordsets:add_element(Node, List);
        Other ->
            ?LOG_ERROR(#{
                what => cets_get_other_nodes_failed, node => Node, table => Table, reason => Other
            }),
            []
    end.

-spec discovery_works(cets_discovery:system_info()) -> boolean().
discovery_works(#{last_get_nodes_result := {ok, _}}) ->
    true;
discovery_works(_) ->
    false.
