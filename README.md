Erleans
=====

[![CircleCI](https://circleci.com/gh/erleans/erleans.svg?style=svg)](https://circleci.com/gh/erleans/erleans)[![codecov](https://codecov.io/gh/erleans/erleans/branch/master/graph/badge.svg)](https://codecov.io/gh/erleans/erleans)

Erleans is a framework for building distributed applications in Erlang and Elixir based on [Microsoft Orleans](https://dotnet.github.io/orleans/).

## Requirements

Rebar3 3.13.0 or above or Elixir 1.9+. Easiest way to get the latest Rebar3:

``` shell
$ rebar3 local upgrade
...
$ export PATH=~/.cache/rebar3/bin:$PATH
```

## Components

### Grains

Stateful grains are backed by persistent storage and referenced by a primary key set by the grain. An activation of a grain is a single Erlang process in on an Erlang node (silo) in an Erlang cluster. Activation placement is handled by Erleans and communication is over standard Erlang distribution. If a grain is sent a message and does not have a current activation one is spawned.

Grain state is persisted through a database provider with an always increasing change id or etag. If the change id or etag has been by another activation the activation attempting to save state will stop.

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

[Streams](https://github.com/erleans/erleans_streams) have a provider type as well for providing a pluggable stream layer.

## Differences from gen_server

No starting or linking, a grain is activated when it is sent a request if an activation is not currently running.

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

## Failure Semantics

## Contributing

### Running Tests

Because the tests rely on certain configurations of apps it is easiest to run the tests with the alias `test`:

```
$ epmd -daemon
$ rebar3 test
```

Note that the distributed tests will only work on a node with hostname `fanon`. This will be easily avoidable in the next release of OTP but until need to figure out another work around. Running `rebar3 ci` will run all the tests but dist tests.
