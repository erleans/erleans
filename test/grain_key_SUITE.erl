-module(grain_key_SUITE).
-behaviour(erleans_grain).
-export([all/0, init_per_suite/1, end_per_suite/1,
         codec/1, invalid_keys/1, reference_validation/1, uuid_identity/1,
         provider_exact_keys/1, key_type/0, placement/0, provider/0,
         state/1, handle_call/3, handle_cast/2]).
-include_lib("eunit/include/eunit.hrl").

all() -> [codec, invalid_keys, reference_validation, uuid_identity, provider_exact_keys].
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erleans),
    Config.
end_per_suite(_) ->
    application:unset_env(erleans, test_key_type),
    application:stop(erleans).

key_type() -> application:get_env(erleans, test_key_type, string).
placement() -> prefer_local.
provider() -> default.
state(Id) -> #{id => Id}.
handle_call(id, From, State = #{id := Id}) -> {ok, State, [{reply, From, Id}]}.
handle_cast(_, State) -> {ok, State}.

uuid() -> <<16#550e8400e29b41d4a716446655440000:128>>.
uuid_text() -> <<"550E8400-E29B-41D4-A716-446655440000">>.

codec(_) ->
    Cases = [{string, <<>>, <<>>, <<>>},
             {string, <<"hello:world/", 16#c3, 16#a9>>, <<"hello:world/", 16#c3, 16#a9>>,
                      <<"hello:world/", 16#c3, 16#a9>>},
             {integer, -9223372036854775808, -9223372036854775808, <<"-9223372036854775808">>},
             {integer, 9223372036854775807, 9223372036854775807, <<"9223372036854775807">>},
             {uuid, uuid_text(), uuid(), <<"550e8400-e29b-41d4-a716-446655440000">>},
             {uuid, binary:encode_hex(uuid()), uuid(), <<"550e8400-e29b-41d4-a716-446655440000">>},
             {integer_compound, {-42, <<"a:b/c">>}, {-42, <<"a:b/c">>}, <<"-42:a:b/c">>},
             {uuid_compound, {uuid_text(), <<"a:b">>}, {uuid(), <<"a:b">>},
                 <<"550e8400-e29b-41d4-a716-446655440000:a:b">>}],
    lists:foreach(fun({Type, Input, Normalized, Encoded}) ->
        ?assertEqual(Normalized, erleans_grain_key:normalize(Type, Input)),
        ?assertEqual(Encoded, erleans_grain_key:encode(Type, Normalized)),
        ?assertEqual(Normalized, erleans_grain_key:decode(Type, Encoded))
    end, Cases),
    Full = binary:copy(<<"a">>, 512),
    ?assertEqual(Full, erleans_grain_key:encode(string, Full)),
    Extension = binary:copy(<<"a">>, 510),
    ?assertEqual(512, byte_size(erleans_grain_key:encode(integer_compound, {1, Extension}))).

invalid_keys(_) ->
    Cases = [{string, I} || I <- [atom, "list", #{}, {a, b}, make_ref(), self(), 1, 1.0,
                                 <<0>>, <<255>>, <<16#c3>>, binary:copy(<<"a">>, 513)]] ++
            [{integer, I} || I <- [<<"42">>, 1.0, 1 bsl 63, -(1 bsl 63) - 1]] ++
            [{uuid, I} || I <- [<<"bad">>, uuid_text(), binary:encode_hex(uuid()), {uuid, uuid()}]] ++
            [{uuid_compound, {uuid_text(), <<"tenant">>}},
             {integer_compound, {42, <<>>}}, {uuid_compound, {uuid(), <<0>>}},
             {integer_compound, {1, binary:copy(<<"a">>, 511)}}],
    %% Invoke dynamically to test values intentionally outside the public types.
    lists:foreach(fun({Type, Input}) ->
        ?assertError({invalid_grain_key, Type, Input}, apply(erleans_grain_key, validate, [Type, Input]))
    end, Cases),
    lists:foreach(fun({Type, Input}) ->
        ?assertError({invalid_encoded_grain_key, Type, Input}, erleans_grain_key:decode(Type, Input))
    end, [{integer, <<"01">>}, {integer, <<"+1">>}, {integer, <<"-0">>},
          {integer_compound, <<"1:">>}, {integer_compound, <<"1">>},
          {uuid, uuid_text()}, {uuid, uuid()}, {string, <<255>>}]).

reference_validation(_) ->
    ?assertEqual(string, erleans:key_type(fault_test_grain)),
    BadId = make_ref(),
    ?assertError({invalid_grain_key, string, BadId},
                 apply(erleans, get_grain, [fault_test_grain, BadId])),
    ?assertError({invalid_grain_key, string, 42}, erleans:get_grain(fault_test_grain, 42)),
    application:set_env(erleans, test_key_type, unsupported),
    try
        ?assertError({invalid_grain_key_type, unsupported}, erleans:get_grain(?MODULE, <<"id">>))
    after application:unset_env(erleans, test_key_type) end.

uuid_identity(_) ->
    application:set_env(erleans, test_key_type, uuid),
    Ref = erleans:get_grain(?MODULE, uuid()),
    try
        Text = uuid_text(),
        ?assertError({invalid_grain_key, uuid, Text}, erleans:get_grain(?MODULE, Text)),
        ?assertEqual(Ref, erleans:get_grain(?MODULE, erleans_grain_key:normalize(uuid, Text))),
        ?assertEqual(uuid(), erleans_grain:call(Ref, id)),
        %% A different representation does not alias a registered identity.
        ?assertEqual(undefined, erleans_grain_registry:whereis_name(Ref#{id := Text})),
        %% References already constructed do not consult key_type/0 on use.
        application:set_env(erleans, test_key_type, unsupported),
        Pid = erleans_grain_registry:whereis_name(Ref),
        ?assert(is_pid(Pid)),
        Changed = Ref#{placement := random, provider := undefined},
        ?assertEqual(Pid, erleans_grain_registry:whereis_name(Changed)),
        ?assertEqual({error, {already_started, Pid}}, erleans_grain_sup:start_child(Changed)),
        ?assertEqual(erleans:identity(Ref), erleans:identity(Changed)),
        ?assertEqual(uuid(), erleans_grain:call(Changed, id)),
        ok = gen_statem:stop(Pid, shutdown, infinity)
    after application:unset_env(erleans, test_key_type) end.

provider_exact_keys(_) ->
    application:set_env(erleans, test_key_type, uuid_compound),
    Key = {uuid(), <<"provider">>},
    Input = {uuid_text(), <<"provider">>},
    P = erleans_provider_ets,
    try
        ?assertEqual({ok, 1}, P:insert(?MODULE, in_memory, Key, a)),
        ?assertEqual({error, bad_etag}, P:insert(?MODULE, in_memory, Key, b)),
        ?assertEqual({error, not_found}, P:read(?MODULE, in_memory, Input)),
        ?assertEqual({ok, a, 1}, P:read(?MODULE, in_memory, Key)),
        ?assertEqual({ok, 2}, P:update(?MODULE, in_memory, Key, b, 1)),
        Hash = erleans_grain_key:hash(?MODULE, Key),
        ?assertEqual({ok, [{Key, ?MODULE, 2, b}]}, P:read_by_hash(?MODULE, in_memory, Hash)),
        true = P:delete(?MODULE, in_memory, Key),
        ?assertEqual({error, not_found}, P:read(?MODULE, in_memory, Key))
    after application:unset_env(erleans, test_key_type) end.
