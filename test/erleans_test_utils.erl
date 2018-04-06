-module(erleans_test_utils).

-export([produce/2]).

produce(Topic, Records) ->
    case application:get_env(erleans, db_testing, false) of
        false ->
            %% ets tables aren't shared, so ned to do this on every node in the cluster
            [rpc:call(N, test_stream, produce, [[{Topic, Records}]]) || N <- [node() | nodes()]];
        _ ->
            {ok, HWM} = vg_client:produce(Topic, Records),
            ct:pal("wrote records to vgt, hwm = ~p", [HWM])
    end.
