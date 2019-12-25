-module(erleans_providers).

-behaviour(gen_server).

-export([start_link/0,
         default/0,
         provider/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2]).

-include_lib("kernel/include/logger.hrl").

-record(state, {providers :: term()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

default() ->
    persistent_term:get({?MODULE, default_provider}).

provider(Name) ->
    persistent_term:get({?MODULE, configured_provider, Name}, undefined).

init([]) ->
    ConfiguredProviders = init_providers(),
    {ok, #state{providers=ConfiguredProviders}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

init_providers() ->
    case erleans_config:get(default_provider, undefined) of
        undefined ->
            ?LOG_ERROR("error=no_default_provider"),
            erlang:error(no_default_provider);
        DefaultProvider ->
            persistent_term:put({?MODULE, default_provider}, DefaultProvider),
            Providers = erleans_config:get(providers, #{}),
            ConfiguredProviders =
                maps:map(fun(ProviderName, ProviderConfig) ->
                                 start_and_set_provider(ProviderName, ProviderConfig)
                         end, Providers),
            persistent_term:put({?MODULE, configured_providers}, ConfiguredProviders)
    end.

start_and_set_provider(Name, Config=#{module := Module}) ->
    %% start provider as child of erleans_provider_sup
    case erleans_provider_sup:start_child(Name, Config) of
        %% handle crash here so application stops with good error message?
        {ok, _Pid} ->
            persistent_term:put({?MODULE, configured_provider, Name}, Module);
        {error, Reason} ->
            ?LOG_ERROR("failed to initialize provider ~s: reason=~p",
                       [Name, Reason]),
            ok
    end.
