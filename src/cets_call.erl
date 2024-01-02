%% @doc Helper module for extended gen_server API.
-module(cets_call).

-export([long_call/2, long_call/3]).
-export([async_operation/2]).
-export([sync_operation/2]).
-export([send_leader_op/2]).
-export([wait_responses/2]).

-include_lib("kernel/include/logger.hrl").

-type request_id() :: cets:request_id().

-type op() :: cets:op().
%% Operations are messages which could be buffered when a server is paused.
%% Operations are also broadcasted to the whole cluster.

-type server_ref() :: cets:server_ref().
-type long_msg() :: cets:long_msg().
-type ok_or_error() :: ok | {error, Reason :: term()}.

%% @doc Makes gen_server:call with better error reporting.
%% It would log a warning if the call takes too long.
-spec long_call(server_ref(), long_msg()) -> term().
long_call(Server, Msg) ->
    long_call(Server, Msg, #{msg => Msg}).

-spec long_call(server_ref(), long_msg(), map()) -> term().
long_call(Server, Msg, Info) ->
    case where(Server) of
        Pid when is_pid(Pid) ->
            Info2 = Info#{server => Server, pid => Pid, node => node(Pid)},
            F = fun() -> gen_server:call(Pid, Msg, infinity) end,
            cets_long:run_tracked(Info2, F);
        undefined ->
            error({pid_not_found, Server})
    end.

%% Contacts the local server to broadcast multinode operation.
%% Returns immediately.
%% You can wait for response from all nodes by calling wait_response/2.
-spec async_operation(server_ref(), op()) -> request_id().
async_operation(Server, Op) ->
    gen_server:send_request(Server, {op, Op}).

-spec sync_operation(server_ref(), op()) -> ok.
sync_operation(Server, Op) ->
    ok = gen_server:call(Server, {op, Op}, infinity).

-spec maybe_sync_operation(server_ref(), op()) -> ok_or_error().
maybe_sync_operation(Server, Op) ->
    gen_server:call(Server, {op, Op}, infinity).

-spec where(server_ref()) -> pid() | undefined.
where(Pid) when is_pid(Pid) -> Pid;
where(Name) when is_atom(Name) -> whereis(Name);
where({global, Name}) -> global:whereis_name(Name);
where({local, Name}) -> whereis(Name);
where({via, Module, Name}) -> Module:whereis_name(Name).

%% Wait around 15 seconds before giving up
%% (we don't count how much we spend calling the leader though)
%% If fails - this means there are some major issues
backoff_intervals() ->
    [10, 50, 100, 500, 1000, 5000, 5000].

%% Sends all requests to a single node in the cluster
-spec send_leader_op(server_ref(), op()) -> ok_or_error().
send_leader_op(Server, Op) ->
    send_leader_op(Server, Op, backoff_intervals()).

send_leader_op(Server, Op, Backoff) ->
    Leader = cets:get_leader(Server),
    Res = maybe_sync_operation(Leader, {leader_op, Op}),
    case Res of
        {error, {wrong_leader, ExpectedLeader}} ->
            ?LOG_WARNING(#{
                what => wrong_leader,
                server => Server,
                operation => Op,
                called_leader => Leader,
                expected_leader => ExpectedLeader
            }),
            %% This could happen if a new node joins the cluster.
            %% So, a simple retry should help.
            case Backoff of
                [Milliseconds | NextBackoff] ->
                    timer:sleep(Milliseconds),
                    send_leader_op(Server, Op, NextBackoff);
                [] ->
                    error(send_leader_op_failed)
            end;
        _ ->
            Res
    end.

%% @doc Waits for multiple responses at once
-spec wait_responses([gen_server:request_id()], cets:response_timeout()) ->
    [cets:response_return()].
wait_responses([ReqId], Timeout) ->
    [gen_server:receive_response(ReqId, Timeout)];
wait_responses(ReqIds, Timeout) when is_integer(Timeout) ->
    Start = erlang:monotonic_time(millisecond),
    wait_responses(ReqIds, {abs, Start + Timeout});
wait_responses(ReqIds, {abs, _} = Timeout) ->
    [gen_server:receive_response(ReqId, Timeout) || ReqId <- ReqIds];
wait_responses(ReqIds, infinity) ->
    [gen_server:receive_response(ReqId, infinity) || ReqId <- ReqIds].
