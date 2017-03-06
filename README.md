erleans
=====

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activiation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activiation one is spawned.

Grain state is persisted through a database provider with an always increasing change id or etag. If the change id or etag has been by another activiation the activation attempting to save state will stop.

Activations are registered through [lasp_pg](https://github.com/lasp-lang/lasp_pg.git).

### Stateless Grains

Stateless grains have no restriction on the number of activations and do not persist state to a database.

Stateless grain activations are registered locally through [gproc](https://github.com/uwiger/gproc/).

### Reminders

Timers that are associated with a grain, meaning if a grain is not active but a reminder for that grain ticks the grain is activated at that time and the reminder is delivered.

### Observers

Processes can subscribe to grains to receive notifications for grain specific events. If a grain supports observers a group is created through `lasp_pg` that observers are added to and to which notifications are sent.

### Providers

Interface that must be implemented for any persistent store to be used for grains.

Streams have a provider type as well for providing a pluggable stream layer.

### Streams

Grains can subscribe to streams and implement callbacks for handling records as they are published by implementing the `erleans_subscriber` behaviour.

## Differences from gen_server

No starting or linking, a grain is activated when it is sent a request if a activiation is not currently running.

### Grain Placement

* `prefer_local`: If an activation does not exist this causes the new activation to be on the same node making the request.
* `random`: Picks a random node to create any new activation of a grain.
* `stateless`: Stateless grains are always local. If no local activation to the request exists one is created.
* `{stateless, Max :: integer()}`: Allows for up to `Max` number of activations for a grain to exist per node. Meaning a new activation until `Max` exist on the node will be created for each request. Preferaby in the future we can have this only create a new activation if the current one is busy. Possibly through `sboker`.

### Example

The grain implementation `test_grain` is found in `test/`:

```erlang
-module(test_grain).

-behaviour(erleans_grain).

...

placement() ->
    prefer_local.

provider() ->
    ets_provider.

deactivated_counter(Ref) ->
    erleans_grain:call(Ref, deactivated_counter).

activated_counter(Ref) ->
    erleans_grain:call(Ref, activated_counter).

node(Ref) ->
    erleans_grain:call(Ref, node).

init(State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{}};
init(State=#{}) ->
    {ok, State#{activated_counter => 1}, #{}}.
```

```erlang
$ rebar3 as test shell
...
> Grain1 = erleans:get_grain(test_grain, <<"grain1">>).
> test_grain:activated_counter(Grain1).
{ok, 1}
```

## Failure Semantics

## Contributing

### Running Tests

Because the tests rely on certain configurations of apps it is easiest to run the tests with the alias `test`:

```
$ epmd -daemon
$ rebar3 test
```
