-module(cets_ping).
-export([ping/1, ping_pairs/1]).

-ifdef(TEST).
-export([net_family/1]).
-endif.

ping(Node) when is_atom(Node) ->
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
                _ ->
                    connect_ping(Node)
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
