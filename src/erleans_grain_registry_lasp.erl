%%%----------------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc Erleans Grain registry backed by lasp.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_grain_registry_lasp).

-export([register_name/2,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1,
         send/2]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("lasp_pg/include/lasp_pg.hrl").

-dialyzer({nowarn_function, register_name/2}).
-dialyzer({nowarn_function, unregister_name/2}).

-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    case lasp_pg:join(Name, Pid, true) of
        {ok, _} ->
            %% Set up a callback to be triggered on a single node (lasp handles this)
            %% when the number of pids registered for this name goes above 1
            EnforceFun = fun(AwSet) -> deactivate_dups(Name, AwSet) end,
            lasp:enforce_once({term_to_binary(Name), ?SET}, {strict, {cardinality, 1}}, EnforceFun),
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
    ?LOG_INFO("at=deactivate_dups name=~p size=~p keep=~p", [Name, Size, Keep]),
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
unregister_name(Name, _Pid) ->
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
    case gproc:where(?stateful(GrainRef)) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
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
            end
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
