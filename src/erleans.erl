%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans).

-export([get_grain/2]).

-type provider() :: module().

-opaque grain_ref() :: #{grain_type := atom(),
                         id := any(),
                         provider => provider()}.

-type grain_type() :: single_activation | stateless | {stateless, integer()}.

-export_type([grain_ref/0,
              grain_type/0,
              provider/0]).

-spec get_grain(atom(), any()) -> grain_ref().
get_grain(GrainType, Id) ->
    case provider(GrainType) of
        {ok, Provider} ->
            #{grain_type => GrainType,
              provider => Provider,
              id => Id};
        undefined ->
            #{grain_type => GrainType,
              id => Id}
    end.

-spec provider(module()) -> provider() | undefined.
provider(Module) ->
    case erlang:function_exported(Module, provider, 0) of
        true ->
            {ok, Module:provider()};
        false ->
            undefined
    end.
