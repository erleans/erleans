erleans [![CircleCI](https://circleci.com/gh/GOFactory/erleans.svg?style=svg)](https://circleci.com/gh/GOFactory/erleans)
=====

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activiation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activiation one is spawned.

Grain state is persisted through a database provider with an always increasing change id or etag. If the change id or etag has been by another activiation the activation attempting to save state will stop.

Activations are registered through [lasp_pg](https://github.com/lasp-lang/lasp_pg.git).

### Stateless Grains

Stateless grains have no restriction on the number of activations and do not persist state to a database.

Stateless grain activations are pooled through [sbroker](https://github.com/fishcakez/sbroker/) while being counted by a [gproc](https://github.com/uwiger/gproc/) resource counter. This allows for the use of sbroker to select an activation if available and to create a new activation if none were available immediately and the number currently activated is less than the max allowed.

### Reminders (TODO)

Timers that are associated with a grain, meaning if a grain is not active but a reminder for that grain ticks the grain is activated at that time and the reminder is delivered.

### Observers (TODO)

Processes can subscribe to grains to receive notifications for grain specific events. If a grain supports observers a group is created through `lasp_pg` that observers are added to and to which notifications are sent.

### Providers

Interface that must be implemented for any persistent store to be used for grains.

Streams have a provider type as well for providing a pluggable stream layer.

### Streams

Grains can subscribe to streams and implement callbacks for handling records as they are published. Each erleans node runs a configurable number of stream agents which are responsible for the actual fetching off a stream and forwarding to the subscribed grains. Stream providers implement how to read from and publish to a given stream backend. On arrival of new events on the stream the agents makes a call to each subscriber with the new event, thus blocking for as long as the slowest subscriber takes to handle the event, before fetching any new events.

A grain must explicitly unsubscribe from a stream or it will continue to receive calls even after being deactivated. This is because subscriptions are based on the grain reference, not the activation, so if a subscribed grain has deactivated it will be reactivated on the next event on the stream.

## Differences from gen_server

No starting or linking, a grain is activated when it is sent a request if an activiation is not currently running.

### Grain Placement

* `prefer_local`: If an activation does not exist this causes the new activation to be on the same node making the request.
* `random`: Picks a random node to create any new activation of a grain.
* `stateless`: Stateless grains are always local. If no local activation to the request exists one is created up to a default maximum value.
* `{stateless, Max :: integer()}`: Allows for up to `Max` number of activations for a grain to exist per node. A new activation, up until `Max` exist on the node, will be created for a request if an existing activation is not currently busy.

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

init(_Grainref, State=#{activated_counter := Counter}) ->
    {ok, State#{activated_counter => Counter+1}, #{}};
init(_Grainref, State=#{}) ->
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

Note that the distributed tests will only work on a node with hostname `fanon`. This will be easily avoidable in the next release of OTP but until need to figure out another work around. Running `rebar3 ci` will run all the tests but dist tests.
