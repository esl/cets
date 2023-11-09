-module(cets_ping).
-export([ping/1]).
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
            case dist_util:split_node(Node) of
                {node, _Name, Host} ->
                    case {inet:getaddr(Host, inet6), inet:getaddr(Host, inet)} of
                        {{error, _}, {error, _}} ->
                            pang;
                        _ ->
                            ping_without_disconnect(Node)
                    end;
                _ ->
                    pang
            end
    end.

%% net_adm:ping/1 but without disconnect_node
%% (because disconnect_node could introduce more chaos and it is not atomic)
ping_without_disconnect(Node) ->
    Msg = {is_auth, node()},
    Dst = {net_kernel, Node},
    try gen:call(Dst, '$gen_call', Msg, infinity) of
        {ok, yes} ->
            pong;
        _ ->
            % erlang:disconnect_node(Node),
            pang
    catch
        _:_ ->
            pang
    end.
