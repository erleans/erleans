-module(erleans_grain_key).

-moduledoc """
Grain key validation and canonical storage encoding.

Strings are exact UTF-8 binaries (no Unicode normalization), without NUL.
UUID keys are 16 bytes; their storage spelling is lowercase hyphenated hex.
normalize/2 is an opt-in helper accepting 32 or 36 hexadecimal UUID characters.
Runtime operations use canonical keys as supplied without normalizing them.
Compound extensions are nonempty strings. Encodings are at most 512 bytes.
The grain module's key_type/0 determines interpretation; it defaults to string.
""".

-export([type/1, validate/2, normalize/2, encode/2, decode/2, hash/2]).
-export_type([key_type/0, key/0, int64/0]).

-type key_type() :: string | integer | uuid | integer_compound | uuid_compound.
-type int64() :: -9223372036854775808..9223372036854775807.
-type key() :: int64() | binary() | {int64() | binary(), binary()}.

-spec type(module()) -> key_type().
type(Module) ->
    Type = erleans_utils:fun_or_default(Module, key_type, string),
    check_type(Type).

%% Validate once at reference construction, without changing the caller's key.
-spec validate(key_type(), key()) -> ok.
validate(Type, Key) ->
    check_type(Type),
    try
        ok = validate_(Type, Key),
        true = byte_size(encode_(Type, Key)) =< 512,
        ok
    catch
        error:_ -> error({invalid_grain_key, Type, Key})
    end.

%% Explicit conversion at an application boundary, never on a runtime path.
-spec normalize(key_type(), key()) -> key().
normalize(Type, Input) ->
    check_type(Type),
    try
        Key = case {Type, Input} of
                  {uuid, _} -> parse_uuid(Input);
                  {uuid_compound, {Base, Extension}} -> {parse_uuid(Base), Extension};
                  _ -> Input
              end,
        ok = validate(Type, Key),
        Key
    catch
        error:_ -> error({invalid_grain_key, Type, Input})
    end.

%% Providers receive canonical keys. Encoding is serialization, not validation.
-spec encode(key_type(), key()) -> binary().
encode(Type, Key) -> encode_(Type, Key).

-spec decode(key_type(), binary()) -> key().
decode(Type, Encoded) ->
    check_type(Type),
    try
        true = is_binary(Encoded) andalso byte_size(Encoded) =< 512,
        Key = decode_(Type, Encoded),
        ok = validate(Type, Key),
        %% Only accept our canonical spelling, so stored keys cannot alias.
        true = Encoded =:= encode_(Type, Key),
        Key
    catch
        error:_ -> error({invalid_encoded_grain_key, Type, Encoded})
    end.

-spec hash(module(), key()) -> non_neg_integer().
hash(Module, Key) ->
    erlang:phash2({Key, Module}).

-spec check_type(term()) -> key_type().
check_type(Type) when Type =:= string; Type =:= integer; Type =:= uuid;
                           Type =:= integer_compound; Type =:= uuid_compound -> Type;
check_type(Type) -> error({invalid_grain_key_type, Type}).

validate_(string, Key) when is_binary(Key), byte_size(Key) =< 512 ->
    nomatch = binary:match(Key, <<0>>),
    true = Key =:= unicode:characters_to_binary(Key, utf8, utf8),
    ok;
validate_(integer, Key) when is_integer(Key),
                             Key >= -9223372036854775808,
                             Key =< 9223372036854775807 -> ok;
validate_(uuid, <<_:128>>) -> ok;
validate_(integer_compound, {Base, Extension}) ->
    ok = validate_(integer, Base),
    extension(Extension);
validate_(uuid_compound, {Base, Extension}) ->
    ok = validate_(uuid, Base),
    extension(Extension).

extension(Extension) when Extension =/= <<>> -> validate_(string, Extension).

parse_uuid(<<_:128>> = Key) -> Key;
parse_uuid(<<_:256>> = Hex) -> binary:decode_hex(Hex);
parse_uuid(<<A:8/binary, $-, B:4/binary, $-, C:4/binary, $-,
             D:4/binary, $-, E:12/binary>>) ->
    binary:decode_hex(<<A/binary, B/binary, C/binary, D/binary, E/binary>>).

encode_(string, Key) -> Key;
encode_(integer, Key) -> integer_to_binary(Key);
encode_(uuid, <<_:128>> = Key) ->
    <<A:8/binary, B:4/binary, C:4/binary, D:4/binary, E:12/binary>> =
        binary:encode_hex(Key, lowercase),
    <<A/binary, $-, B/binary, $-, C/binary, $-, D/binary, $-, E/binary>>;
encode_(integer_compound, {Base, Extension}) ->
    <<(encode_(integer, Base))/binary, $:, Extension/binary>>;
encode_(uuid_compound, {Base, Extension}) ->
    <<(encode_(uuid, Base))/binary, $:, Extension/binary>>.

decode_(string, Key) -> Key;
decode_(integer, Key) -> binary_to_integer(Key);
decode_(uuid, Key) -> parse_uuid(Key);
decode_(integer_compound, Key) ->
    [Base, Extension] = binary:split(Key, <<":">>),
    {binary_to_integer(Base), Extension};
decode_(uuid_compound, Key) ->
    [Base, Extension] = binary:split(Key, <<":">>),
    {parse_uuid(Base), Extension}.
