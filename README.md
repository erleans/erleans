Erleans
=====

[![Common Test](https://github.com/erleans/erleans/actions/workflows/ct.yml/badge.svg)](https://github.com/erleans/erleans/actions/workflows/ct.yml)[![codecov](https://codecov.io/gh/erleans/erleans/branch/main/graph/badge.svg)](https://codecov.io/gh/erleans/erleans)

Erleans is a framework for building distributed applications in Erlang and Elixir based on [Microsoft Orleans](https://dotnet.github.io/orleans/).

## Requirements

[Rebar3](http://rebar3.org/) 3.24.0 or above or [Elixir](https://elixir-lang.org/) 1.18+. 

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activation one is spawned.

Grain state is persisted through a storage provider which owns its change id or ETag. The grain treats the ETag as an opaque token and passes it back on each save. Providers must atomically reject stale ETags and return a new token on every successful write, even when the state is unchanged. If another activation has changed the ETag, the activation attempting to save state exits with `saved_etag_changed` unless a save continuation reloads its state. The built-in ETS provider uses an always increasing integer version per row, starting at 1.

Activations are registered through
[global](https://www.erlang.org/doc/apps/kernel/global.html) by default.
The registration is retained until `deactivate/1` and any state save it requests
have completed, so a replacement activation reads the completed save.
When disconnected directories merge with duplicate activations, a custom resolver
selects one owner and stops the loser with `{shutdown, duplicate_activation}`.
The loser skips `deactivate/1` and state saving and does not unregister the winner.
Pending calls through grain references can then re-route to the surviving owner.

Grain startup acknowledges the supervisor as soon as registration completes
(including pool registration for stateless grains). Storage reads and `activate/2`
then run concurrently across grains, with requests queued until activation finishes.
An activation returning `{error, notfound}`, or initial state of `notfound`, exits
with reason `notfound`; waiting calls exit with `{notfound, {gen_statem, call, _}}`
without retrying.

Exceptions raised by `handle_call/3` are returned to the calling process and
re-raised with their original class, reason, and callback stacktrace. The
activation keeps its last successfully returned state and continues processing
queued requests. Exceptions in `handle_cast/2` and `handle_info/2` are logged and
also leave the activation running. These exceptions do not invoke deactivation
or automatically save state; external side effects performed before the exception
are not rolled back. Activation failures, invalid callback results, and unhandled
action errors remain fatal. A `save_state` continuation can handle a provider's
error result and explicitly reload after a conflict, as described below.

Calls through grain references re-resolve the activation after transport exits
caused by normal termination, `noproc`, `shutdown`, `{shutdown, _}`,
`{nodedown, _}`, or `noconnection`. This applies to stateful and stateless grains.
There are at most five retries, separated by 10 milliseconds, and attempts use
the remaining call timeout rather than resetting it. Stateless pool waits also
respect the remaining budget. Calls directly to a PID cannot be re-routed.
Callback exceptions, request timeouts, storage conflicts, and explicit activation
errors are not retried. Casts remain asynchronous and do not acknowledge delivery.

A transport failure can happen after a request has executed but before its reply
arrives. Retrying that request can repeat side effects; operations which need to
avoid duplicates should use application-level request IDs and deduplication.

### Grain keys

Each grain module declares one key type with an optional `key_type/0` callback.
The default is `string`. Callers pass untagged values to `get_grain/2`:

| `key_type/0` | Example input |
| --- | --- |
| `string` (default) | `<<"alice">>` |
| `integer` | `42` |
| `uuid` | A 16-byte UUID binary |
| `integer_compound` | `{42, <<"tenant-a">>}` |
| `uuid_compound` | `{UUID, <<"tenant-a">>}` |

For example, an Erlang grain using UUID keys exports:

```erlang
key_type() -> uuid.
```

An Elixir grain declares the same contract with `use Erleans.Grain, key_type: :uuid`.
Integers must fit a signed 64-bit value. Strings are UTF-8 binaries without NUL;
Erlang character lists are not accepted. Empty string keys are allowed, but
compound extensions must be nonempty. Strings retain their exact bytes, including
case and Unicode composition. Every canonical encoded key is limited to 512 bytes.

UUID keys must be 16 raw bytes, including inside compound keys. Callers can
explicitly convert textual UUIDs at their application boundary:

```erlang
UUID = erleans_grain_key:normalize(uuid, <<"550E8400-E29B-41D4-A716-446655440000">>),
Ref = erleans:get_grain(player_grain, UUID).
```

The optional `normalize/2` helper accepts 32 hexadecimal digits or 36-byte
hyphenated UUID text in either case. Runtime operations never call it.
`get_grain/2` validates the supplied key without converting it. Thereafter calls,
casts, activation, registry and pool lookups, and hashing use that key directly.
Construct references through `get_grain/2`; code that manually builds or changes a
reference is responsible for preserving its canonical key. Different raw keys
are different identities and are not silently repaired. Direct provider callers
must also supply canonical keys. Providers serialize keys without normalizing or
revalidating them. Stored text is validated when decoded by `decode/2`.

Invalid keys raise `{invalid_grain_key, KeyType, Input}`; unsupported declarations
raise `{invalid_grain_key_type, Type}`. Atoms, floats, maps, PIDs, references, and
arbitrary tuples are not grain keys.

Logical identity is the implementing module plus the canonical key. Registry
names and stateless pool names use `erleans:identity/1`, independently of provider
and placement metadata. Each module must keep its key type consistent on all nodes.

Providers can use `erleans_grain_key:encode/2` and `decode/2` with the module's
`erleans:key_type/1`. String keys are stored verbatim, integers as decimal, UUIDs
as lowercase hyphenated text, and compound keys as `Base:Extension`. Only the
first colon separates a compound key; colons in the extension are preserved.
The encoding has no type tags because the grain module supplies its interpretation.
`decode/2` rejects noncanonical spellings with
`{invalid_encoded_grain_key, KeyType, Encoded}`. Default lookup hashes use
`erleans_grain_key:hash(Module, Key)`, which hashes the supplied key unchanged.

This is a breaking identity change. Replace old arbitrary-term IDs with supported
keys and migrate persisted IDs before upgrading. Stop old activations on every
node before deploying the new core and providers: registry and pool names have
changed. Changing an existing module's key type also requires migration. Grain
payload serialization and provider-owned ETags are independent of key encoding.

### Stateless Grains

Stateless grains have no restriction on the number of activations and do not persist state to a database.

Stateless grain activations are pooled through [gproc](https://github.com/uwiger/gproc/).

### Reminders (TODO)

Timers that are associated with a grain, meaning if a grain is not active but a reminder for that grain ticks the grain is activated at that time and the reminder is delivered.

### Observers (TODO)

Processes can subscribe to grains to receive notifications for grain specific
events. If a grain supports observers a group is created through
[pg](https://www.erlang.org/doc/apps/kernel/pg.html).

### Providers

Interface that must be implemented for any persistent store to be used for grains.

Storage providers generate ETags; grains never compute them from the payload:

* `read(Type, ProviderName, Id)` returns `{ok, State, ETag}` or `{error, not_found}`. Other read errors stop activation.
* `insert(Type, ProviderName, Id, State)` atomically inserts only when no row exists for that type and id, and returns `{ok, ETag}`. A competing insert returns `{error, bad_etag}` and stops activation.
* `update(Type, ProviderName, Id, State, ETag)` atomically checks the stored ETag, writes the state, and returns `{ok, NewETag}`. A stale ETag returns `{error, bad_etag}`; a missing row must also be rejected, using `{error, not_found}` or `{error, bad_etag}`.
* The explicit-hash variants are `insert(Type, ProviderName, Id, Hash, State)` and `update(Type, ProviderName, Id, Hash, State, ETag)`. This hash is a lookup aid, not an ETag.

`undefined` is reserved for state that has not been read or inserted. Providers may use integer, binary, or other token representations. Tokens must not be reused across successful writes to an existing row, including when the payload changes from A to B and back to A.

This changes the provider API: remove the ETag input from `insert` and the caller-computed new ETag input from `update`, and return `{ok, ETag}` instead of `ok`. Existing providers must be updated before use with this version.

The separate [PostgreSQL provider](https://github.com/erleans/erleans_provider_pgo) uses a database-owned version column, as in [Orleans PostgreSQL persistence](https://github.com/dotnet/orleans/blob/main/src/AdoNet/Orleans.Persistence.AdoNet/PostgreSQL-Persistence.sql): a conditional insert returns version `1`, and an update uses `SET version = version + 1 WHERE ... AND version = $expected RETURNING version`. Uniqueness on the complete grain key prevents concurrent inserts from both succeeding. The database version is returned as the ETag, and a failed condition maps to `{error, bad_etag}`.

After a successful first activation, a missing row is conditionally inserted
using the initial persistent state from `state/1` (or `#{}` when it is absent).
Changes returned by `activate/2` remain in memory, on both first and subsequent
activations, until a `save_state` action or a `{save_state, State}` result from
`deactivate/1` persists them. Failed activation does not insert a row.

[Streams](https://github.com/erleans/erleans_streams) have a provider type as well for providing a pluggable stream layer.

### Save continuations and conflict recovery

Return `save_state` to persist the state returned by the callback. Return
`{save_state, Fun}` to handle the result in the grain process before another
message is processed. The function receives `ok` or `{error, Reason}`; provider
conflicts (`bad_etag` and `{bad_etag, Expected, Stored}`) become
`{error, saved_etag_changed}`.

```erlang
handle_call({set, Value}, From, State) ->
    NewState = State#{value => Value},
    OnSave = fun
        (ok) ->
            {continue, [{reply, From, ok}]};
        ({error, saved_etag_changed}) ->
            {reload, fun(CurrentState) ->
                {continue, [{reply, From, {error, {conflict, CurrentState}}}]}
            end};
        ({error, Reason}) ->
            {stop, [{reply, From, {error, {save_failed, Reason}}}]}
    end,
    {ok, NewState, [{save_state, OnSave}]}.
```

The continuation returns one of:

* `{continue, Actions}` after a successful write. The returned state and new
  ETag are adopted. Actions can be `reply`, `cast`, or `info` actions.
* `{stop, Replies}` to reply and terminate without calling `deactivate/1` or
  saving again. Only reply actions are accepted. The exit reason is
  `saved_etag_changed` for a conflict, `{save_failed, Reason}` for another write
  error, or `{save_stopped, ok}` after a successful write or reload.
* `{reload, ReloadFun}` after a conflict. Erleans reads the current persistent
  state and ETag, then calls `ReloadFun(CurrentState)`. That function returns
  `{continue, Actions}` or `{stop, Replies}`. Continuing keeps the same activation
  alive; its next save uses the newly read ETag.

Reload discards the rejected persistent state and any earlier unsaved persistent
changes. For `{Ephemeral, Persistent}` state, it preserves the ephemeral state
from **before the failed callback** and replaces only the persistent part.
`activate/2` is not called again. The rejected operation is not automatically
retried. An explicit `deactivate` callback result still requests deactivation
after recovery; use `ok` as in the example to keep the activation active.

Each callback result may contain at most one save action. Other actions in the
same list run only on write success, even if they appear before the save. On
success with `continue`, continuation actions run at the save action's position;
on failure, all sibling actions are discarded. A `stop` result executes only its
returned replies. Continuations return actions, not another state, and cannot
return additional save actions.

A bare `save_state` conflict terminates the process with `saved_etag_changed`.
Waiting `erleans_grain:call` callers retain the `{exit, saved_etag_changed}`
result. Other provider errors can be handled with `{stop, Replies}`; continuing
without a successful reload is disallowed. Reload is limited to conflicts:
an arbitrary storage error, such as a timeout, does not establish whether a
write committed and is not safely resolved by simply reading once.

A failed reload (including a missing row) terminates with
`{state_reload_failed, Reason}`. Exceptions in either continuation are fatal
and do not trigger save-on-deactivation or automatic request replay. A successful
write remains committed if its continuation fails. These hooks are not durable:
the process can fail between the write and the continuation. Use a persisted
outbox when a follow-up effect must survive process failure.

## Differences from gen_server

No starting or linking, a grain is activated when it is sent a request if an activation is not currently running.

### Grain Placement

* `prefer_local`: If an activation does not exist this causes the new activation to be on the same node making the request.
* `random`: Picks a random node to create any new activation of a grain.
* `stateless`: Stateless grains are always local. If no local activation to the request exists one is created up to a default maximum value.
* `{stateless, Max :: integer()}`: Allows for up to `Max` number of activations for a grain to exist per node. A new activation, up until `Max` exist on the node, will be created for a request if an existing activation is not currently busy.

`get_grain/2` normalizes `stateless` to `{stateless, N}`, where `N` is the
`default_stateless_max` setting (5 by default). Unsupported placement values,
including the removed `system_grain` placement, raise `{invalid_placement, Value}`
when building the grain reference.

### Erlang Example

The grain implementation `test_grain` is found in `test/`:

```erlang
-module(test_grain).

-behaviour(erleans_grain).

...

placement() ->
    prefer_local.

provider() ->
    in_memory.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

node(Ref) ->
    erleans_grain:call(Ref, node).

state(_) ->
    #{activated_counter => 0,
      deactivated_counter => 0}.

activate(_, State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{}}.
```

```erlang
$ rebar3 as test shell
...
> Grain1 = erleans:get_grain(test_grain, <<"grain1">>).
> test_grain:activated_counter(Grain1).
{ok, 1}
```

## Elixir Example

Configure the built-in ETS provider in `config/config.exs`:

```elixir
import Config

config :erleans,
  providers: %{in_memory: %{module: :erleans_provider_ets, args: %{}}},
  default_provider: :in_memory
```

This provider stores state in memory and does not survive node restarts. Put the
grain implementation in `lib/erleans_elixir_example.ex`:

``` elixir
defmodule ErleansElixirExample do
  use Erleans.Grain,
    placement: :prefer_local,
    provider: :in_memory,
    state: %{:counter => 0}

  def get(ref) do
    :erleans_grain.call(ref, :get)
  end

  def increment(ref) do
    :erleans_grain.cast(ref, :increment)
  end

  def handle_call(:get, from, state = %{:counter => counter}) do
    {:ok, state, [{:reply, from, counter}]}
  end

  def handle_cast(:increment, state = %{:counter => counter}) do
    new_state = %{state | :counter => counter + 1}
    {:ok, new_state, [:save_state]}
  end
end
```

``` elixir
$ mix deps.get
$ mix compile
$ iex --sname a@localhost -S mix

iex(a@localhost)1> ref = Erleans.get_grain(ErleansElixirExample, "somename")
...
iex(a@localhost)2> ErleansElixirExample.get(ref)
0
iex(a@localhost)3> ErleansElixirExample.increment(ref)
:ok
iex(a@localhost)4> ErleansElixirExample.get(ref)
1
```

## Contributing

### Running Tests

```
$ epmd -daemon
$ rebar3 ct
$ MIX_ENV=test mix deps.get
$ mix test
```
