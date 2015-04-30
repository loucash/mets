# mets
This application implements online aggregation and storage system for metrics.
It does not store raw data and is meant to be used in a single machine/node
system or on an embedded system.

This is still under development and it wasn't used in production.

Metrics storage backend is exchangeable. Right now `mets` supports only `sqlite`,
but it is fairly easy to define another one.

Aggregate function supported by `mets`: `sum`, `max`, `min`, `avg`, `count`.
Please create an issue if some other are needed.

## How to build it
`make deps compile`

## How to run tests
`make tests`

## Tutorial

Metric (data point) consists of:
* namespace
* name
* timestamp
* value
* tags (optional)

Aggregates are defined in config file (`rollups` key) as list of pattern matching
rules `{Ns, Name, Tags, Aggregate, Options}`, where:
* `Ns`, `Name` or `Tags` can be a specific value or `'_'` which means any value
* `Aggregate` is `{Function, Tick}` e.g. `{avg, {15, minutes}}`
* `Options` is a list of options supported by backend, for know sqlite supports ttl: `[{ttl, 60}]`
  (in seconds).

## API

* writing metrics:

  `mets:push(Ns, Name, Timestamp, Value).`

  `mets:push(Ns, Name, Tags, Timestamp, Value).`

  where arguments are of type:
  * `Ns`, `Name` - binary
  * `Tags` - proplist
  * `Timestamp` - integer (milliseconds)
  * `Value` - number

* reading metrics:

  `mets:search(Options)`

  where `Options` is a proplist and options are:
  * `ns` - required
  * `name` - required
  * `tags` - optional
  * `start_time` - required; milliseconds
  * `end_time` - optional; milliseconds
  * `order` - optional; possible values: `asc` (default) or `desc`
  * `aggregate` - requires; which aggregate to read from, e.g. `{avg, {15, minutes}}`

Example session:
```erlang
%
% configuration:
% {rollups, [{'_', '_', '_', {avg, {15, minutes}}, [{ttl, 86400}]}]}
%
1> mets:start().
16:48:28.325 [info] Application lager started on node mets@feynman
16:48:28.336 [info] Application mets started on node mets@feynman
ok
2> mets:push(<<"ns">>, <<"name">>, 1431013663000, 1).
ok
3> mets:push(<<"ns">>, <<"name">>, 1431013664000, 2).
ok
4> mets:push(<<"ns">>, <<"name">>, 1431013665000, 3).
ok
5> mets:push(<<"ns">>, <<"name">>, 1431013666000, 4).
ok
6> mets:search([{ns, <<"ns">>}, {name, <<"name">>}, {start_time, 1431013500000}, {aggregate, {avg, {15, minutes}}}]).
{ok,[[{tags,[]},{ts,1431013500000},{value,2.5}]]}
```

## Configuration options

* `rollups_emit_tick` - how often partial results are saved

* `rollups_procs_ttl` - how long process that aggregates single metrics waits
  for new data

## Backends

Supported backends: sqlite.

You can choose a backend in configuration: `{backend, mets_backend_sqlite}`.

`mets_backend_sqlite` backend support additional options:

  * `data_file` - which is a path and a name of sqlite db file.
  * `delete_tick` - how often delete outdated data points (with ttl)

To add a new backend, `mets_backend` callbacks should be implemented:

```erlang
% initiates backend
-spec init() -> ok.

% returns single aggregate value (that was emitted by mets_aggregator)
-spec fetch(ns(), name(), tags(), milliseconds(), aggregate()) ->
    {ok, any()} | {error, not_found}.

% saves aggregate
-spec push(ns(), name(), tags(), milliseconds(), aggregate(), aggregate_state(), backend_options()) ->
    ok | {error, any()}.

% returns search results
-spec search(search()) -> {ok, list()} | {error, any()}.
```

## Contributing

If you see something missing or incorrect, do not hesitate to create an issue
or pull request. Thank you!

## Roadmap

- consider using hdr_histogram
- proper test for mets_backend behaviour
- stress tests
- add overload control

## Authors

- Lukasz Biedrycki / @loucash: current implementation
