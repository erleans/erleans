Erleans
=====

[![Common Test](https://github.com/erleans/erleans/actions/workflows/ct.yml/badge.svg)](https://github.com/erleans/erleans/actions/workflows/ct.yml)[![codecov](https://codecov.io/gh/erleans/erleans/branch/main/graph/badge.svg)](https://codecov.io/gh/erleans/erleans)

Erleans is a framework for building distributed applications in Erlang and Elixir based on [Microsoft Orleans](https://dotnet.github.io/orleans/).

## Requirements

[Rebar3](http://rebar3.org/) 3.24.0 or above or [Elixir](https://elixir-lang.org/) 1.18+. 

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activation one is spawned.

Grain state is persisted through a storage provider which owns its change id or ETag. The grain treats the ETag as an opaque token and passes it back on each save. Providers must atomically reject stale ETags and return a new token on every successful write, even when the state is unchanged. If another activation has changed the ETag, the activation attempting to save state will stop. The built-in ETS provider uses an always increasing integer version per row, starting at 1.

Activations are registered through
[global](https://www.erlang.org/doc/apps/kernel/global.html) by default.
The registration is retained until `deactivate/1` and any state save it requests
have completed, so a replacement activation reads the completed save.
When disconnected directories merge with duplicate activations, a custom resolver
selects one owner and stops the loser with `{shutdown, duplicate_activation}`.
The loser skips `deactivate/1` and state saving and does not unregister the winner.
Pending calls through grain references can then re-route to the surviving owner.

Exceptions raised by `handle_call/3` are returned to the calling process and
re-raised with their original class, reason, and callback stacktrace. The
activation keeps its last successfully returned state and continues processing
queued requests. Exceptions in `handle_cast/2` and `handle_info/2` are logged and
also leave the activation running. These exceptions do not invoke deactivation
or automatically save state; external side effects performed before the exception
are not rolled back. Activation failures, invalid callback results, and errors
while executing actions such as `save_state` remain fatal.

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

