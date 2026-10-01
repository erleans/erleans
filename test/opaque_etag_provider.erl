%% A test provider with binary ETags and injected I/O failures. The runtime must
%% preserve its tokens across insert, save, shutdown, and later activation.
-module(opaque_etag_provider).
-behaviour(erleans_provider).

-export([start_link/2, all/2, read/3, read_by_hash/3,
         insert/4, insert/5, update/5, update/6]).

start_link(Name, Args) -> erleans_provider_ets:start_link(Name, Args).

all(Type, Name) ->
    {ok, Rows} = erleans_provider_ets:all(Type, Name),
    {ok, [{Id, T, Hash, encode(Version), State} || {Id, T, Hash, Version, State} <- Rows]}.

read(_, _, read_failure) -> {error, read_failed};
read(Type, Name, Id) ->
    case erleans_provider_ets:read(Type, Name, Id) of
        {ok, State, Version} -> {ok, State, encode(Version)};
        Error -> Error
    end.

read_by_hash(Type, Name, Hash) ->
    {ok, Rows} = erleans_provider_ets:read_by_hash(Type, Name, Hash),
    {ok, [{Id, T, encode(Version), State} || {Id, T, Version, State} <- Rows]}.

insert(_, _, insert_failure, _) -> {error, insert_failed};
insert(Type, Name, Id, State) ->
    result(erleans_provider_ets:insert(Type, Name, Id, State)).

insert(Type, Name, Id, Hash, State) ->
    result(erleans_provider_ets:insert(Type, Name, Id, Hash, State)).

update(_, _, write_failure, _, _) -> {error, write_failed};
update(Type, Name, Id = {deactivation_save, Test, Tag}, State = #{value := saved}, ETag) ->
    Test ! {Tag, saving, self()},
    receive {Tag, finish_save} -> ok end,
    result(erleans_provider_ets:update(Type, Name, Id, State, decode(ETag)));
update(Type, Name, Id, State, ETag) ->
    result(erleans_provider_ets:update(Type, Name, Id, State, decode(ETag))).

update(Type, Name, Id, Hash, State, ETag) ->
    result(erleans_provider_ets:update(Type, Name, Id, Hash, State, decode(ETag))).

result({ok, Version}) -> {ok, encode(Version)};
result(Error) -> Error.

encode(Version) -> <<"version:", (integer_to_binary(Version))/binary>>.
decode(<<"version:", Version/binary>>) -> binary_to_integer(Version).
