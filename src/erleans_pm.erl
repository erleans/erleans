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
            EnforceFun = fun(AwSet) -> deactivate_dups(Name, AwSet) end,
            lasp:enforce_once({term_to_binary(Name), ?SET}, {cardinality, 2}, EnforceFun),
            yes;
        _ ->
            no
    end.

%% deactivate all but a random activation.
%% It is possible that this will be triggered multiple times and result in no
%% remaining activations until the next request to a grain.
deactivate_dups(Name, AwSet) ->
    Set = state_awset:query(AwSet),
    Size = sets:size(Set),
    Keep = rand:uniform(Size),
    lager:info("at=deactivate_dups name=~p size=~p keep=~p", [Name, Size, Keep]),
    sets:fold(fun(_, N) when N =:= Size ->
                  N+1;
                 (Pid, N) ->
                  supervisor:terminate_child({erleans_grain_sup, node(Pid)}, Pid),
                  N+1
              end, 1, Set).


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

-spec whereis_name(GrainRef :: erleans:grain_ref()) -> pid() | undefined.
whereis_name(GrainRef=#{placement := stateless}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef=#{placement := {stateless, _}}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef) ->
    case lasp_pg:members(GrainRef) of
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

whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            Pid
    end.
