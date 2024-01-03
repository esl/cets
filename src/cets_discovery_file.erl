%% @doc File backend for cets_discovery.
%%
%% Barebone AWS EC2 auto-discovery is limited:
%%
%% - UDP broadcasts do not work.
%%
%% - AWS CLI needs access.
%%
%% - DNS does not allow to list subdomains.
%%
%% So, we use a file with nodes to connect as a discovery mechanism
%% (so, you can hardcode nodes or use your method of filling it).
-module(cets_discovery_file).
-behaviour(cets_discovery).
-export([init/1, get_nodes/1]).

-include_lib("kernel/include/logger.hrl").

-type opts() :: #{disco_file := file:filename()}.
%% Start options.

-type state() :: opts().
%% The backend state.

%% @doc Init backend.
-spec init(opts()) -> state().
init(Opts) ->
    Opts.

%% @doc Get a list of nodes.
-spec get_nodes(state()) -> {cets_discovery:get_nodes_result(), state()}.
get_nodes(State = #{disco_file := Filename}) ->
    case file:read_file(Filename) of
        {error, Reason} ->
            Log = #{
                what => discovery_failed,
                filename => Filename,
                reason => Reason
            },
            ?LOG_ERROR(Log),
            {{error, Reason}, State};
        {ok, Text} ->
            Lines = binary:split(Text, [<<"\r">>, <<"\n">>, <<" ">>], [global]),
            Nodes = [binary_to_atom(X, latin1) || X <- Lines, X =/= <<>>],
            {{ok, Nodes}, State}
    end.
