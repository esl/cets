-module(cets_status).
-export([status/1]).

%% RPC callbacks
-export([get_local_table_to_other_nodes_map_from_disco/1]).
-ignore_xref([status/1, get_local_table_to_other_nodes_map_from_disco/1]).

-include_lib("kernel/include/logger.hrl").

-type tab_nodes_map() :: #{Table :: atom() => Nodes :: ordsets:ordset(node())}.
-type node_to_tab_nodes_map() :: #{node() => tab_nodes_map()}.
-type table_name() :: cets:table_name().
-type disco_name() :: atom().

-type info() :: #{
    %% Nodes that are connected to us and have the CETS disco process started.
    available_nodes := [node()],
    %% Nodes that do not respond to our pings.
    unavailable_nodes := [node()],
    %% Nodes that has our local tables running (but could also have some unknown tables).
    %% All joined nodes replicate data between each other.
    joined_nodes := [node()],
    %% Nodes that are extracted from the discovery backend.
    discovered_nodes := [node()],
    %% True, if discovery backend returned the list of nodes the last time we've tried
    %% to call it.
    discovery_works := boolean(),
    %% Nodes with stopped disco
    remote_nodes_without_disco := [node()],
    %% Nodes that have more tables registered than the local node.
    remote_nodes_with_unknown_tables := [node()],
    remote_unknown_tables := [table_name()],
    %% Nodes that are available, but do not host one of our local tables.
    remote_nodes_with_missing_tables => [node()],
    remote_missing_tables := [table_name()],
    %% Nodes that replicate at least one of our local tables to a different list of nodes
    %% (could temporary happen during a netsplit)
    conflict_nodes := [node()],
    conflict_tables := [table_name()]
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
    Expected = get_local_table_to_other_nodes_map(Tables),
    OtherTabNodes = get_node_to_tab_nodes_map(AvailNodes, Disco),
    JoinedNodes = joined_nodes(Expected, OtherTabNodes),
    AllTables = all_tables(Expected, OtherTabNodes),
    {UnknownTables, NodesWithUnknownTables} = unknown_tables(OtherTabNodes, Tables, AllTables),
    {MissingTables, NodesWithMissingTables} = missing_tables(OtherTabNodes, Tables),
    {ConflictTables, ConflictNodes} = conflict_tables(Expected, OtherTabNodes),
    #{
        available_nodes => AvailNodes,
        unavailable_nodes => UnNodes,
        joined_nodes => JoinedNodes,
        discovered_nodes => DiscoNodesSorted,
        discovery_works => discovery_works(Info),
        remote_nodes_without_disco => NoDiscoNodes,
        remote_unknown_tables => UnknownTables,
        remote_missing_tables => MissingTables,
        remote_nodes_with_unknown_tables => NodesWithUnknownTables,
        remote_nodes_with_missing_tables => NodesWithMissingTables,
        conflict_nodes => ConflictNodes,
        conflict_tables => ConflictTables
    }.

%% Nodes, that host the discovery process
-spec available_nodes(disco_name(), [node(), ...]) -> [node()].
available_nodes(Disco, OnlineNodes) ->
    Results = erpc:multicall(OnlineNodes, erlang, whereis, [Disco], infinity),
    [Node || {Node, {ok, Pid}} <- lists:zip(OnlineNodes, Results), is_pid(Pid)].

remote_nodes_without_disco(DiscoNodes, AvailNodes, OnlineNodes) ->
    lists:filter(fun(Node) -> is_node_without_disco(Node, AvailNodes, OnlineNodes) end, DiscoNodes).

is_node_without_disco(Node, AvailNodes, OnlineNodes) ->
    lists:member(Node, OnlineNodes) andalso not lists:member(Node, AvailNodes).

-spec get_node_to_tab_nodes_map(AvailNodes, Disco) -> OtherTabNodes when
    AvailNodes :: [node()],
    Disco :: disco_name(),
    OtherTabNodes :: node_to_tab_nodes_map().
get_node_to_tab_nodes_map(AvailNodes, Disco) ->
    OtherNodes = lists:delete(node(), AvailNodes),
    F = get_local_table_to_other_nodes_map_from_disco,
    Results = erpc:multicall(OtherNodes, ?MODULE, F, [Disco], infinity),
    OtherTabNodes = [{Node, Result} || {Node, {ok, Result}} <- lists:zip(OtherNodes, Results)],
    maps:from_list(OtherTabNodes).

%% Nodes that has our local tables running (but could also have some unknown tables).
%% All joined nodes replicate data between each other.
-spec joined_nodes(tab_nodes_map(), node_to_tab_nodes_map()) -> [node()].
joined_nodes(Expected, OtherTabNodes) ->
    ExpectedTables = maps:keys(Expected),
    OtherJoined = maps:fold(
        fun(Node, TabNodes, Acc) ->
            case maps:with(ExpectedTables, TabNodes) =:= Expected of
                true -> [Node | Acc];
                false -> Acc
            end
        end,
        [],
        OtherTabNodes
    ),
    lists:sort([node() | OtherJoined]).

unknown_tables(OtherTabNodes, Tables, AllTables) ->
    UnknownTables = ordsets:subtract(AllTables, Tables),
    NodesWithUnknownTables =
        maps:fold(
            fun(Node, TabNodes, Acc) ->
                case tabnodes_has_any_of(TabNodes, UnknownTables) of
                    true -> [Node | Acc];
                    false -> Acc
                end
            end,
            [],
            OtherTabNodes
        ),
    {UnknownTables, NodesWithUnknownTables}.

-spec missing_tables(node_to_tab_nodes_map(), [table_name()]) -> {[table_name()], [node()]}.
missing_tables(OtherTabNodes, LocalTables) ->
    Zip = maps:fold(
        fun(Node, TabNodes, Acc) ->
            RemoteTables = maps:keys(TabNodes),
            MissingTables = ordsets:subtract(LocalTables, RemoteTables),
            case MissingTables of
                [] -> Acc;
                [_ | _] -> [{MissingTables, Node} | Acc]
            end
        end,
        [],
        OtherTabNodes
    ),
    {MissingTables, NodesWithMissingTables} = lists:unzip(Zip),
    {lists:usort(lists:append(MissingTables)), NodesWithMissingTables}.

-spec tabnodes_has_any_of([table_name()], [table_name()]) -> boolean().
tabnodes_has_any_of(TabNodes, UnknownTables) ->
    lists:any(fun(Tab) -> maps:is_key(Tab, TabNodes) end, UnknownTables).

%% Nodes that replicate at least one of our local tables to a different list of nodes
%% (could temporary happen during a netsplit)
-spec conflict_tables(tab_nodes_map(), node_to_tab_nodes_map()) -> {[table_name()], [node()]}.
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

-spec all_tables(tab_nodes_map(), node_to_tab_nodes_map()) -> [table_name()].
all_tables(Expected, OtherTabNodes) ->
    TableNodesVariants = [Expected | maps:values(OtherTabNodes)],
    TableVariants = lists:map(fun maps:keys/1, TableNodesVariants),
    ordsets:union(TableVariants).

%% Returns nodes for each table hosted on node()
-spec get_local_table_to_other_nodes_map_from_disco(disco_name()) -> tab_nodes_map().
get_local_table_to_other_nodes_map_from_disco(Disco) ->
    {ok, Tables} = cets_discovery:get_tables(Disco),
    get_local_table_to_other_nodes_map(Tables).

%% Returns nodes for each table in the Tables list
-spec get_local_table_to_other_nodes_map([table_name()]) -> tab_nodes_map().
get_local_table_to_other_nodes_map(Tables) ->
    RequestIds = lists:map(fun cets:get_nodes_request/1, Tables),
    Responses = cets:wait_responses(RequestIds, infinity),
    Zip = lists:zip(Tables, Responses),
    [
        begin
            ?LOG_ERROR(#{what => get_nodes_request_failed, table => Table, reason => Reason}),
            ok
        end
     || {Table, {error, Reason}} <- Zip
    ],
    %% Skip bad replies
    maps:from_list([{Table, Nodes} || {Table, {reply, Nodes}} <- Zip, is_list(Nodes)]).

-spec discovery_works(cets_discovery:system_info()) -> boolean().
discovery_works(#{last_get_nodes_result := {ok, _}}) ->
    true;
discovery_works(_) ->
    false.