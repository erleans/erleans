-module(erleans_provider_ets).

-behaviour(erleans_provider).
-behaviour(gen_server).

-export([start_link/2,
         all/2,
         read/3,
         read_by_hash/3,
         insert/4,
         insert/5,
         update/5,
         update/6,
         delete/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

start_link(ProviderName, Args) ->
    gen_server:start_link({local, ProviderName}, ?MODULE, [ProviderName, Args], []).

all(Type, ProviderName) ->
    try
        {ok, [{Id, Type, Hash, ETag, Object} ||
                 {{_, Id}, _, Hash, ETag, Object} <-
                     ets:match_object(ProviderName, {'_', Type, '_', '_', '_'})]}
    catch
        error:badarg ->
            {error, missing_table}
    end.

read(Type, ProviderName, Id) ->
    case ets:lookup(ProviderName, {Type, Id}) of
        [{{Type, Id}, Type, _Hash, ETag, Object}] ->
            {ok, Object, ETag};
        _ ->
            {error, not_found}
    end.

read_by_hash(Type, ProviderName, Hash) ->
    {ok, [{Id, Type, ETag, Object} ||
             {{_, Id}, _, _, ETag, Object} <- ets:match_object(ProviderName, {'_', Type, Hash, '_', '_'})]}.

insert(Type, ProviderName, Id, State) ->
    insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), State).

insert(Type, ProviderName, Id, Hash, State) ->
    case ets:insert_new(ProviderName, {{Type, Id}, Type, Hash, 1, State}) of
        true -> {ok, 1};
        false -> {error, bad_etag}
    end.

update(Type, ProviderName, Id, State, ETag) ->
    update(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag).

update(Type, ProviderName, Id, Hash, State, ETag) when is_integer(ETag), ETag > 0 ->
    %% Compare and increment in one ETS operation, including for unchanged data.
    %% Constants keep arbitrary grain ids and payloads out of match-spec syntax.
    Match = [{{'$1', '_', '_', '$2', '_'},
              [{'=:=', '$1', {const, {Type, Id}}}, {'=:=', '$2', {const, ETag}}],
              [{{'$1', {const, Type}, {const, Hash}, {'+', '$2', 1}, {const, State}}}]}],
    case ets:select_replace(ProviderName, Match) of
        1 -> {ok, ETag + 1};
        0 ->
            case ets:member(ProviderName, {Type, Id}) of
                true -> {error, bad_etag};
                false -> {error, not_found}
            end
    end;
update(_Type, _ProviderName, _Id, _Hash, _State, _ETag) ->
    {error, bad_etag}.

delete(Type, ProviderName, Id) ->
    ets:delete(ProviderName, {Type, Id}).


init([ProviderName, _]) ->
    Tid = ets:new(ProviderName, [public, named_table, set, {keypos, 1}]),
    {ok, Tid}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.
