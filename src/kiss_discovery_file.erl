%% AWS auto-discovery is kinda bad.
%% - UDP broadcasts do not work
%% - AWS CLI needs access
%% - DNS does not allow to list subdomains
%% So, we use a file with nodes to connect as a discovery mechanism
%% (so, you can hardcode nodes or use your method of filling it)
-module(kiss_discovery_file).
-behaviour(kiss_discovery).
-export([init/1, get_nodes/1]).

init(Opts) ->
    Opts.

get_nodes(State = #{disco_file := Filename}) ->
    case file:read_file(Filename) of
        {error, Reason} ->
            error_logger:error_msg("what=discovery_failed filename=~0p reason=~0p",
                                   [Filename, Reason]),
            {{error, Reason}, State};
        {ok, Text} ->
            Lines = binary:split(Text, [<<"\r">>, <<"\n">>, <<" ">>], [global]),
            Nodes = [binary_to_atom(X, latin1) || X <- Lines, X =/= <<>>],
            {{ok, Nodes}, State}
    end.
