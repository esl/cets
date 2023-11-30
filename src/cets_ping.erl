-module(cets_ping).
-export([ping/1, ping_pairs/1, pre_connect/1]).

-ifdef(TEST).
-export([net_family/1]).
-endif.

-ignore_xref([pre_connect/1]).

-spec ping(node()) -> pong | pang.
ping(Node) when is_atom(Node) ->
    case lists:member(Node, nodes()) of
        true ->
            pong;
        false ->
            case can_preconnect_from_all_nodes(Node) of
                true ->
                    connect_ping(Node);
                false ->
                    pang
            end
    end.

%% Preconnect checks if the remote node could be connected.
%% It is important to check this on all nodes before actually connecting
%% to avoid getting kicked by overlapped nodes protection in the global module.
%% There are two major scenarios for this function:
%% - Node is down and would return pang on all nodes.
%% - Node is up and would return pong on all nodes.
%% These two scenarios should happen during the normal operation,
%% so code is optimized for them.
%% For first scenario we want to check the local node first and do not do RPCs
%% if possible.
%% For second scenario we want to do the check in parallel on all nodes.
%%
%% This function avoids a corner case when node returns pang on some of the nodes
%% (third scenario).
%% This could be because:
%% - netsplit
%% - node is not resolvable on some of the nodes yet
-spec can_preconnect_from_all_nodes(node()) -> boolean().
can_preconnect_from_all_nodes(Node) ->
    Nodes = nodes(),
    %% pre_connect is safe to run in parallel
    %% (it does not actually create a distributed connection)
    case pre_connect(Node) of
        pang ->
            %% Node is probably down, skip multicall
            false;
        pong ->
            Results = erpc:multicall(Nodes, ?MODULE, pre_connect, [Node], 5000),
            %% We skip nodes which do not have cets_ping module and return an error
            not lists:member({ok, pang}, Results)
    end.

-spec pre_connect(node()) -> pong | pang.
pre_connect(Node) when is_atom(Node) ->
    %% It is important to understand, that initial setup for dist connections
    %% is done by the single net_kernel process.
    %% It calls net_kernel:setup, which calls inet_tcp_dist, which calls
    %% erl_epmd:address_please/3, which does a DNS request.
    %% If DNS is slow - net_kernel process would become busy.
    %% But if we have a lot of offline nodes in the CETS discovery table,
    %% we would try to call net_kernel for each node (even if we probably would receive
    %% {error, nxdomain} from erl_epmd:address_please/3).
    %% So, we first do nslookup here and only after that we try to connect.
    case lists:member(Node, nodes()) of
        true ->
            pong;
        false ->
            {node, Name, Host} = dist_util:split_node(Node),
            Epmd = net_kernel:epmd_module(),
            case Epmd:address_please(Name, Host, net_family()) of
                {error, _} ->
                    pang;
                {ok, IP} ->
                    case can_connect(IP) of
                        true ->
                            pong;
                        false ->
                            pang
                    end
            end
    end.

%% The user should use proto_dist flag to enable inet6.
-spec net_family() -> inet | inet6.
net_family() ->
    net_family(init:get_argument(proto_dist)).

net_family({ok, [["inet6" ++ _]]}) ->
    inet6;
net_family(_) ->
    inet.

connect_ping(Node) ->
    %% We could use net_adm:ping/1 but it:
    %% - disconnects node on pang - we don't want that
    %%   (because it could disconnect an already connected node because of race conditions)
    %% - calls net_kernel's gen_server of the remote server,
    %%   but it could be busy doing something, which means slower response time.
    case net_kernel:connect_node(Node) of
        true ->
            pong;
        _ ->
            pang
    end.

-spec ping_pairs([{node(), node()}]) -> [{node(), node(), pong | Reason :: term()}].
ping_pairs(Pairs) ->
    %% We could use rpc:multicall(Nodes, cets_ping, ping, Args).
    %% But it means more chance of nodes trying to contact each other.
    ping_pairs_stop_on_pang(Pairs).

ping_pairs_stop_on_pang([{Node1, Node2} | Pairs]) ->
    F = fun() -> rpc:call(Node1, cets_ping, ping, [Node2], 10000) end,
    Info = #{task => ping_node, node1 => Node1, node2 => Node2},
    Res = cets_long:run_tracked(Info, F),
    case Res of
        pong ->
            [{Node1, Node2, pong} | ping_pairs_stop_on_pang(Pairs)];
        Other ->
            %% We do not need to ping the rest of nodes -
            %% one node returning pang is enough to cancel join.
            %% We could exit earlier and save some time
            %% (connect_node to the dead node could be time consuming)
            [{Node1, Node2, Other} | fail_pairs(Pairs, skipped)]
    end;
ping_pairs_stop_on_pang([]) ->
    [].

fail_pairs(Pairs, Reason) ->
    [{Node1, Node2, Reason} || {Node1, Node2} <- Pairs].

-spec can_connect(inet:ip_address()) -> boolean().
can_connect(IP) ->
    case gen_tcp:connect(IP, get_epmd_port(), [], 5000) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            true;
        _ ->
            false
    end.

-spec get_epmd_port() -> inet:port_number().
get_epmd_port() ->
    case init:get_argument(epmd_port) of
        {ok, [[PortStr | _] | _]} when is_list(PortStr) ->
            list_to_integer(PortStr);
        error ->
            4369
    end.
