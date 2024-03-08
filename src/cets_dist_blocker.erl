%% @doc Disallow distributed erlang connections until cleaning is done.
%%
%% This module prevents a node from reconnecting, until cleaning activity is
%% finished. It prevents race conditions.
%%
%% This module assumes all nodes share the same cookie.
-module(cets_dist_blocker).
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/0,
    add_cleaner/1,
    cleaning_done/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ignore_xref([
    start_link/0,
    add_cleaner/1,
    cleaning_done/2
]).

-type cleaner_pid() :: pid().
-type waiting() :: [{node(), cleaner_pid()}].

-type state() :: #{
    cleaners := [cleaner_pid()],
    waiting := waiting()
}.

%% @doc Spawn `dist_blocker'
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register CleanerPid as a cleaner.
-spec add_cleaner(pid()) -> ok.
add_cleaner(CleanerPid) ->
    gen_server:call(?MODULE, {add_cleaner, CleanerPid}).

%% @doc Confirm that cleaning is done.
%%
%% This function should be called by a cleaner when it receives
%% nodedown and finishes cleaning.
-spec cleaning_done(pid(), node()) -> ok.
cleaning_done(CleanerPid, Node) ->
    gen_server:call(?MODULE, {cleaning_done, CleanerPid, Node}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    ok = net_kernel:monitor_nodes(true),
    State = #{cleaners => [], waiting => []},
    State2 = lists:foldl(fun handle_nodeup/2, State, nodes()),
    {ok, State2}.

handle_call({add_cleaner, CleanerPid}, _From, State) ->
    {reply, ok, handle_add_cleaner(CleanerPid, State)};
handle_call({cleaning_done, CleanerPid, Node}, _From, State) ->
    {reply, ok, maybe_unblock(State, handle_cleaning_done(CleanerPid, Node, State))};
handle_call(Request, _From, State) ->
    ?LOG_ERROR(#{what => unexpected_call, msg => Request}),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR(#{what => unexpected_cast, msg => Msg}),
    {noreply, State}.

handle_info({nodeup, Node}, State) ->
    {noreply, handle_nodeup(Node, State)};
handle_info({nodedown, Node}, State) ->
    {noreply, handle_nodedown(Node, State)};
handle_info({'DOWN', _Ref, process, Pid, _Info}, State) ->
    {noreply, maybe_unblock(State, handle_cleaner_down(Pid, State))};
handle_info(Info, State) ->
    ?LOG_ERROR(#{what => unexpected_info, msg => Info}),
    {noreply, State}.

terminate(_Reason, State) ->
    %% Restore cookies
    _ = maybe_unblock(State, State#{waiting := []}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

-spec handle_nodeup(node(), state()) -> state().
handle_nodeup(Node, State) ->
    %% We change the cookie as soon as the node is connected.
    %% Alternative is to do it on nodedown, but because nodedown-s are async,
    %% we would have a high chance of race conditions (so, node could reconnect
    %% before we set cookie).
    erlang:set_cookie(Node, blocking_cookie()),
    State.

%% Make cookie, that would prevent node from connecting
-spec blocking_cookie() -> atom().
blocking_cookie() ->
    list_to_atom(atom_to_list(erlang:get_cookie()) ++ "_blocked_by_" ++ atom_to_list(node())).

%% Allow the node to connect to us again
-spec unblock_node(node(), state()) -> state().
unblock_node(Node, State) ->
    erlang:set_cookie(Node, erlang:get_cookie()),
    State.

-spec handle_nodedown(node(), state()) -> state().
handle_nodedown(Node, State = #{cleaners := []}) ->
    %% Skip waiting when no cleaners
    unblock_node(Node, State);
handle_nodedown(Node, State = #{cleaners := Cleaners, waiting := Waiting}) ->
    New = [{Node, CleanerPid} || CleanerPid <- Cleaners],
    State#{waiting := lists:usort(New ++ Waiting)}.

-spec handle_add_cleaner(cleaner_pid(), state()) -> state().
handle_add_cleaner(CleanerPid, State = #{cleaners := Cleaners}) ->
    erlang:monitor(process, CleanerPid),
    State#{cleaners := lists:usort([CleanerPid | Cleaners])}.

-spec handle_cleaning_done(cleaner_pid(), node(), state()) -> state().
handle_cleaning_done(CleanerPid, Node, State = #{waiting := Waiting}) ->
    State#{waiting := lists:delete({Node, CleanerPid}, Waiting)}.

-spec handle_cleaner_down(cleaner_pid(), state()) -> state().
handle_cleaner_down(CleanerPid, State = #{cleaners := Cleaners, waiting := Waiting}) ->
    State#{
        cleaners := lists:delete(CleanerPid, Cleaners),
        waiting := [X || {_Node, CleanerPid2} = X <- Waiting, CleanerPid =/= CleanerPid2]
    }.

%% Unblock nodes when the last cleaner confirms the cleaning is done.
%% Call this function each time you remove entries from the waiting list.
-spec maybe_unblock(state(), state()) -> state().
maybe_unblock(_OldState = #{waiting := OldWaiting}, NewState = #{waiting := NewWaiting}) ->
    OldNodes = cast_waiting_to_nodes(OldWaiting),
    NewNodes = cast_waiting_to_nodes(NewWaiting),
    CleanedNodes = OldNodes -- NewNodes,
    lists:foldl(fun unblock_node/2, NewState, CleanedNodes).

-spec cast_waiting_to_nodes(waiting()) -> [node()].
cast_waiting_to_nodes(Waiting) ->
    lists:usort([Node || {Node, _CleanerPid} <- Waiting]).
