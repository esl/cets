-module(cets_test_wait).
-export([wait_until/2]).

%% Helpers
-export([
    wait_for_name_to_be_free/2,
    wait_for_down/1
]).

%% From mongoose_helper

%% @doc Waits `TimeLeft` for `Fun` to return `ExpectedValue`
%% If the result of `Fun` matches `ExpectedValue`, returns {ok, ExpectedValue}
%% If no value is returned or the result doesn't match `ExpectedValue`, returns one of the following:
%% {Name, History}, if Opts as #{name => Name} is passed
%% {timeout, History}, otherwise

wait_until(Fun, ExpectedValue) ->
    wait_until(Fun, ExpectedValue, #{}).

%% Example: wait_until(fun () -> ... end, SomeVal, #{time_left => timer:seconds(2)})
wait_until(Fun, ExpectedValue, Opts) ->
    Defaults = #{
        validator => fun(NewValue) -> ExpectedValue =:= NewValue end,
        expected_value => ExpectedValue,
        time_left => timer:seconds(5),
        sleep_time => 100,
        history => [],
        name => timeout
    },
    do_wait_until(Fun, maps:merge(Defaults, Opts)).

do_wait_until(
    _Fun,
    #{
        expected_value := ExpectedValue,
        time_left := TimeLeft,
        history := History,
        name := Name
    } = Opts
) when TimeLeft =< 0 ->
    error({Name, ExpectedValue, simplify_history(lists:reverse(History), 1), on_error(Opts)});
do_wait_until(Fun, #{validator := Validator} = Opts) ->
    try Fun() of
        Value ->
            case Validator(Value) of
                true -> {ok, Value};
                _ -> wait_and_continue(Fun, Value, Opts)
            end
    catch
        Error:Reason:Stacktrace ->
            wait_and_continue(Fun, {Error, Reason, Stacktrace}, Opts)
    end.

on_error(#{on_error := F}) ->
    F();
on_error(_Opts) ->
    ok.

simplify_history([H | [H | _] = T], Times) ->
    simplify_history(T, Times + 1);
simplify_history([H | T], Times) ->
    [{times, Times, H} | simplify_history(T, 1)];
simplify_history([], 1) ->
    [].

wait_and_continue(
    Fun,
    FunResult,
    #{
        time_left := TimeLeft,
        sleep_time := SleepTime,
        history := History
    } = Opts
) ->
    timer:sleep(SleepTime),
    do_wait_until(Fun, Opts#{
        time_left => TimeLeft - SleepTime,
        history => [FunResult | History]
    }).

%% Helpers

wait_for_name_to_be_free(Node, Name) ->
    %% Wait for the old process to be killed by the cleaner in schedule_cleanup.
    %% Cleaner is fast, but not instant.
    cets_test_wait:wait_until(
        fun() -> cets_test_rpc:rpc(Node, erlang, whereis, [Name]) end, undefined
    ).

wait_for_down(Pid) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, process, Pid, Reason} -> Reason
    after 5000 -> ct:fail({wait_for_down_timeout, Pid})
    end.
