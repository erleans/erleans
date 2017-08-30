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
-module(erleans_utils).

-export([fun_or_default/3]).
-export([fun_or_default/5]).

%% If a function is exported by the module return the result of calling it
%% else return the default.
-spec fun_or_default(module(), atom(), term()) -> term().
fun_or_default(Module, FunctionName, Default) ->
    fun_or_default(Module, FunctionName, 0, [], Default).

-spec fun_or_default(module(), atom(), integer(), list(), term()) -> term().
fun_or_default(Module, FunctionName, Arity, Args, Default) ->
    %% load the module if it isn't already
    erlang:function_exported(Module, module_info, 0) orelse code:ensure_loaded(Module),
    case erlang:function_exported(Module, FunctionName, Arity) of
        true ->
            erlang:apply(Module, FunctionName, Args);
        false ->
            Default
    end.
