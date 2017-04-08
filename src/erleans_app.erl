%%%-------------------------------------------------------------------
%% @doc erleans public API
%% @end
%%%-------------------------------------------------------------------

-module(erleans_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    init_providers(),
    erleans_dns_peers:join(),
    erleans_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    partisan_peer_service:leave(),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

init_providers() ->
    Providers = erleans_config:get(providers, []),
    [Provider:init(Args) || {Provider, Args} <- Providers].
