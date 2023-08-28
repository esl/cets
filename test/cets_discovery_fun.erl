-module(cets_discovery_fun).
-behaviour(cets_discovery).
-export([init/1, get_nodes/1]).

init(Opts) ->
    Opts.

get_nodes(State = #{get_nodes_fn := F}) ->
    F(State).
