-module(cets_status).
-export([status/1]).
-ignore_xref([status/1]).
-include_lib("kernel/include/logger.hrl").

-type tab_nodes() :: {Table :: atom(), Nodes :: ordsets:ordset(node())}.
-type table_name() :: cets:table_name().
-type disco_name() :: atom().

-type info() :: #{
    unavailable_nodes => [node()],
    available_nodes => [node()],
    joined_nodes => [node()],
    partially_joined_nodes => [node()],
    partially_joined_tables => [table_name()],
    discovered_nodes => [node()],
    discovery_works => boolean()
}.

-spec status(disco_name()) -> info().
status(Disco) when is_atom(Disco) ->
    %% The node lists could not match for different nodes
    %% because they are updated periodically
    #{unavailable_nodes := UnNodes, nodes := Nodes, tables := Tables} =
        Info = cets_discovery:system_info(Disco),
    NodesSorted = lists:sort(Nodes),
    AvailNodes = available_nodes(Disco),
    {JoinedNodes, PartTables} = filter_joined_nodes(AvailNodes, Tables, Disco),
    PartNodes = AvailNodes -- JoinedNodes,
    #{
        unavailable_nodes => UnNodes,
        available_nodes => AvailNodes,
        joined_nodes => JoinedNodes,
        partially_joined_nodes => PartNodes,
        partially_joined_tables => PartTables,
        discovered_nodes => NodesSorted,
        discovery_works => discovery_works(Info)
    }.

%% Nodes, that host the discovery process
-spec available_nodes(disco_name()) -> [node()].
available_nodes(Disco) ->
    OnlineNodes = [node() | nodes()],
    [Node || Node <- OnlineNodes, is_disco_running_on(Node, Disco)].

-spec is_disco_running_on(node(), disco_name()) -> boolean().
is_disco_running_on(Node, Disco) ->
    is_pid(rpc:call(Node, erlang, whereis, [Disco])).

%% Returns only nodes that replicate all our local CETS tables to the same list of remote nodes
%% (and do not have some unknown tables)
-spec filter_joined_nodes(AvailNodes :: [node()], Tables :: [table_name()], Disco :: disco_name()) ->
    {JoinedNodes :: [node()], PartTables :: [table_name()]}.
filter_joined_nodes(AvailNodes, Tables, Disco) ->
    OtherNodes = lists:delete(node(), AvailNodes),
    Expected = node_list_for_tables(node(), Tables),
    OtherTables = [{Node, node_list_for_tables_from_disco(Node, Disco)} || Node <- OtherNodes],
    OtherJoined = [Node || {Node, NodeTabs} <- OtherTables, NodeTabs =:= Expected],
    JoinedNodes = lists:sort([node() | OtherJoined]),
    PartTables = filter_partially_joined_tables(Expected, OtherTables),
    {JoinedNodes, PartTables}.

%% Partially joined means that:
%% - one of the nodes has a table not known to other nodes
%% - or some tables are not joined by all nodes
-spec filter_partially_joined_tables([tab_nodes()], [{node(), [tab_nodes()]}]) -> [table_name()].
filter_partially_joined_tables(Expected, OtherTables) ->
    TableNodesVariants = [Expected | [NodeTabs || {_Node, NodeTabs} <- OtherTables]],
    TableVariants = lists:map(fun tab_nodes_to_tables/1, TableNodesVariants),
    SharedTables = ordsets:intersection(TableVariants),
    AllTables = ordsets:union(TableVariants),
    ordsets:subtract(AllTables, SharedTables).

-spec tab_nodes_to_tables([tab_nodes()]) -> [table_name()].
tab_nodes_to_tables(TabNodes) ->
    [Table || {Table, [_ | _] = _Nodes} <- TabNodes].

%% Returns nodes for each table hosted on node()
-spec node_list_for_tables_from_disco(node(), disco_name()) -> [tab_nodes()].
node_list_for_tables_from_disco(Node, Disco) ->
    Tables = get_tables_list_on_node(Node, Disco),
    node_list_for_tables(Node, Tables).

%% Returns nodes for each table in the Tables list
-spec node_list_for_tables(node(), [table_name()]) -> [tab_nodes()].
node_list_for_tables(Node, Tables) ->
    [{Table, node_list_for_table(Node, Table)} || Table <- Tables].

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
