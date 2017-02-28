%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc Erleans Grain process manager.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_pm).

-export([register_name/2,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1,
         send/2]).

-include_lib("lasp_pg/include/lasp_pg.hrl").

-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    case lasp_pg:join(Name, Pid, true) of
        {ok, _} ->
            %% Set up a callback to be triggered on a single node (lasp handles this)
            %% when the number of pids registered for this name goes above 1
            EnforceFun = fun(AwSet) -> deactivate(Name, AwSet) end,
            lasp:enforce_once({term_to_binary(Name), ?SET}, {cardinality, 2}, EnforceFun),
            yes;
        _ ->
            no
    end.

%% deactivate all but one of the activations for this single activation
%% needs to be deterministic but shouldn't just stop all but the first in the
%% sorted list as it does now either.
deactivate(Name, AwSet) ->
    Set = state_awset:query(AwSet),
    [_Keep | Rest] = lists:usort(sets:to_list(Set)),
    lager:info("at=enforce_once_deactivate name=~p keep=~p rest=~p", [Name, _Keep, Rest]),
    [supervisor:terminate_child({erleans_grain_sup, node(StopPid)}, StopPid) || StopPid <- Rest].

-spec unregister_name(Name :: term()) -> Name :: term() | fail.
unregister_name(Name) ->
    case ?MODULE:whereis_name(Name) of
        Pid when is_pid(Pid) ->
            unregister_name(Name, Pid);
        undefined ->
            undefined
    end.

-spec unregister_name(Name :: term(), Pid :: pid()) -> Name :: term() | fail.
unregister_name(Name, Pid) ->
    case lasp_pg:leave(Name, Pid) of
        {ok, _} ->
            Name;
        _ ->
            fail
    end.

-spec whereis_name(Name :: term()) -> pid() | undefined.
whereis_name(Name) ->
    case lasp_pg:members(Name) of
        {ok, Set} ->
            case sets:to_list(Set) of
                [Pid | _] ->
                    Pid;
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

-spec send(Name :: term(), Message :: term()) -> pid().
send(Name, Message) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Message;
        undefined ->
            error({badarg, Name})
    end.
