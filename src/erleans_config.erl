%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_config).

-export([get/1,
         get/2,
         provider/1]).

-spec get(atom()) -> any().
get(Key) ->
    {ok, Value} = application:get_env(erleans, Key),
    Value.

-spec get(atom(), any()) -> any().
get(Key, Default) ->
    application:get_env(erleans, Key, Default).

provider(Grain) ->
    Value = application:get_env(erleans, providers_mapping, []),
    case proplists:get_value(Grain, Value, undefined) of
        undefined ->
            case application:get_env(erleans, default_provider) of
                {ok, Provider} ->
                    Provider;
                _ ->
                    lager:error("error=no_default_provider"),
                    erlang:error(no_default_provider)
            end;
        Provider ->
            Provider
    end.
