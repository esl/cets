%% @doc Module for extending gen_server calls.
%% Also, it contains code for sync and async multinode operations.
%% Operations are messages which could be buffered when a server is paused.
%% Operations are also broadcasted to the whole cluster.
-module(cets_call).

-export([long_call/2, long_call/3]).
-export([async_operation/2]).
-export([sync_operation/2]).
-export([reply/1]).

-type request_id() :: cets:request_id().
-type op() :: cets:op().
-type server_ref() :: cets:server_ref().
-type long_msg() :: cets:long_msg().

%% Does gen_server:call with better error reporting.
%% It would log a warning if the call takes too long.
-spec long_call(server_ref(), long_msg()) -> term().
long_call(Server, Msg) ->
    long_call(Server, Msg, #{msg => Msg}).

-spec long_call(server_ref(), long_msg(), map()) -> term().
long_call(Server, Msg, Info) ->
    case where(Server) of
        Pid when is_pid(Pid) ->
            Info2 = Info#{
                remote_server => Server,
                remote_pid => Pid,
                remote_node => node(Pid)
            },
            F = fun() -> gen_server:call(Pid, Msg, infinity) end,
            cets_long:run_safely(Info2, F);
        undefined ->
            {error, pid_not_found}
    end.

%% Contacts the local server to broadcast multinode operation.
%% Returns immediately.
%% You can wait for response from all nodes by calling wait_response/2.
-spec async_operation(server_ref(), op()) -> request_id().
async_operation(Server, Msg) ->
    gen_server:send_request(Server, {op, Msg}).

-spec sync_operation(server_ref(), op()) -> ok.
sync_operation(Server, Msg) ->
    gen_server:call(Server, {op, Msg}).

-spec where(server_ref()) -> pid() | undefined.
where(Pid) when is_pid(Pid) -> Pid;
where(Name) when is_atom(Name) -> whereis(Name);
where({global, Name}) -> global:whereis_name(Name);
where({local, Name}) -> whereis(Name);
where({via, Module, Name}) -> Module:whereis_name(Name).

reply(Alias) ->
    %% Pid part is not used
    gen_server:reply({x, [alias | Alias]}, ok).
