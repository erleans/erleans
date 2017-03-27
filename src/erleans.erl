%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans).

-export([get_grain/2]).

-include("erleans.hrl").

-type provider() :: module().

-type grain_ref() :: #{implementing_module := module(),
                       id := term(),
                       placement := placement(),
                       provider => provider()}.

-type placement() :: random | prefer_local | stateless | {stateless, integer()}. %% | load

-export_type([grain_ref/0,
              placement/0,
              provider/0]).

-spec get_grain(atom(), any()) -> grain_ref().
get_grain(ImplementingModule, Id) ->
    BaseGrainRef = #{implementing_module => ImplementingModule,
                     placement => placement(ImplementingModule),
                     id => Id},
    case provider(ImplementingModule) of
        undefined ->
            BaseGrainRef;
        Provider ->
            BaseGrainRef#{provider => Provider}
    end.

-spec provider(module()) -> provider() | undefined.
provider(Module) ->
    fun_or_default(Module, provider, undefined).

-spec placement(module()) -> placement().
placement(Module) ->
    fun_or_default(Module, placement, ?DEFAULT_PLACEMENT).

%% If a function is exported by the module return the result of calling it
%% else return the default.
-spec fun_or_default(module(), atom(), term()) -> term().
fun_or_default(Module, FunctionName, Default) ->
    case erlang:function_exported(Module, FunctionName, 0) of
        true ->
            Module:FunctionName();
        false ->
            Default
    end.
