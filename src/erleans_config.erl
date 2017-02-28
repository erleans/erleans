%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_config).

-export([get/1]).

-spec get(atom()) -> any().
get(Key) ->
    {ok, Value} = application:get_env(erleans, Key),
    Value.
