Erleans
=====

[![Common Test](https://github.com/erleans/erleans/actions/workflows/ct.yml/badge.svg)](https://github.com/erleans/erleans/actions/workflows/ct.yml)[![codecov](https://codecov.io/gh/erleans/erleans/branch/main/graph/badge.svg)](https://codecov.io/gh/erleans/erleans)

Erleans is a framework for building distributed applications in Erlang and Elixir based on [Microsoft Orleans](https://dotnet.github.io/orleans/).

## Requirements

[Rebar3](http://rebar3.org/) 3.24.0 or above or [Elixir](https://elixir-lang.org/) 1.18+. 

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activation one is spawned.

Grain state is persisted through a database provider with an always increasing change id or etag. If the change id or etag has been by another activation the activation attempting to save state will stop.

Activations are registered through
[global](https://www.erlang.org/doc/apps/kernel/global.html) by default.

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

[Streams](https://github.com/erleans/erleans_streams) have a provider type as well for providing a pluggable stream layer.

## Differences from gen_server

No starting or linking, a grain is activated when it is sent a request if an activation is not currently running.

### Grain Placement

* `prefer_local`: If an activation does not exist this causes the new activation to be on the same node making the request.
* `random`: Picks a random node to create any new activation of a grain.
* `stateless`: Stateless grains are always local. If no local activation to the request exists one is created up to a default maximum value.
* `{stateless, Max :: integer()}`: Allows for up to `Max` number of activations for a grain to exist per node. A new activation, up until `Max` exist on the node, will be created for a request if an existing activation is not currently busy.

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

``` elixir
defmodule ErleansElixirExample do
  use Erleans.Grain,
    placement: :prefer_local,
    provider: :postgres,
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
```

