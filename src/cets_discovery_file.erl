%% AWS auto-discovery is kinda bad.
%% - UDP broadcasts do not work
%% - AWS CLI needs access
%% - DNS does not allow to list subdomains
%% So, we use a file with nodes to connect as a discovery mechanism
%% (so, you can hardcode nodes or use your method of filling it)
-module(cets_discovery_file).
-behaviour(cets_discovery).
-export([init/1, get_nodes/1]).

-include_lib("kernel/include/logger.hrl").

init(Opts) ->
    Opts.

get_nodes(State = #{disco_file := Filename}) ->
    case file:read_file(Filename) of
        {error, Reason} ->
            ?LOG_ERROR(#{what => discovery_failed,
                         filename => Filename, reason => Reason}),
            {{error, Reason}, State};
        {ok, Text} ->
            Lines = binary:split(Text, [<<"\r">>, <<"\n">>, <<" ">>], [global]),
            Nodes = [binary_to_atom(X, latin1) || X <- Lines, X =/= <<>>],
            {{ok, Nodes}, State}
    end.
