-module(cets_safety).
-export([run/2]).
-include_lib("kernel/include/logger.hrl").

run(Info, Fun) ->
    try
        Fun()
    catch Class:Reason:Stacktrace ->
              ?LOG_ERROR(Info#{class => Class, reason => Reason, stacktrace => Stacktrace}),
              {error, {Class, Reason, Stacktrace}}
    end.
