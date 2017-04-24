%%%--------------------------------------------------------------------
%%% Copyright Space-Time Insight 2017. All Rights Reserved.
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
%%%-----------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans).

-export([get_grain/2,
         get_stream/2,
         get_stream/3]).

-include("erleans.hrl").

-type provider() :: module().

-type grain_ref() :: #{implementing_module := module(),
                       id                  := term(),
                       placement           := placement(),
                       provider            => provider()}.

-type sequence_token() :: any().
-type stream_ref() :: #{topic           := term(),
                        stream_provider := module(),
                        sequence_token  := sequence_token(),
                        fetch_interval  := integer()}.

-type placement() :: random | prefer_local | stateless | {stateless, integer()} | system_grain. %% | load

-type etag() :: integer().

-export_type([grain_ref/0,
              stream_ref/0,
              placement/0,
              provider/0,
              etag/0]).

-spec get_grain(module(), any()) -> grain_ref().
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

-spec get_stream(module(), term()) -> stream_ref().
get_stream(StreamProvider, Topic) ->
    get_stream(StreamProvider, Topic, 0).

-spec get_stream(module(), term(), sequence_token()) -> stream_ref().
get_stream(StreamProvider, Topic, SequenceToken) ->
    #{topic => Topic,
      stream_provider => StreamProvider,
      sequence_token => SequenceToken,
      fetch_interval => ?INITIAL_FETCH_INTERVAL}.

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
    %% load the module if it isn't already
    erlang:function_exported(Module, module_info, 0) orelse code:ensure_loaded(Module),
    case erlang:function_exported(Module, FunctionName, 0) of
        true ->
            Module:FunctionName();
        false ->
            Default
    end.
