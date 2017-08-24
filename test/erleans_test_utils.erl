-module(erleans_test_utils).

-export([produce/2]).

produce(Topic, Records) ->
    case application:get_env(erleans, db_testing, false) of
        false ->
            [true] = test_stream:produce([{Topic, Records}]);
        _ ->
            ok = vg_client_pool:start(),
            {ok, HWM} = vg_client:produce(Topic, Records),
            ct:pal("wrote records to vgt, hwm = ~p", [HWM])
    end.
