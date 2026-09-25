# com-anmandu-connector

A small **Apache Camel + Spring Boot** connector that simulates a stream of sensor
readings and computes rolling averages over them using four different strategies.

## What it does

[`PublisherRoutes`](src/main/java/com/anmandu/connector/PublisherRoutes.java) runs up
to four independent timers (`sensor-A` through `sensor-D`, one per simulated sensor),
each on its own period (`timer.sensorA`..`timer.sensorD` in `application.properties`).
Every tick, [`MockInput`](src/main/java/com/anmandu/connector/MockInput.java) generates
a random measurement tagged with that sensor's name and pushes it into an internal
`seda:myInternalQueue`. Only `sensor-A` runs by default; sensors B-D are gated behind
the `multiple.publishers` flag.

A consumer route in [`MySpringBootRouter`](src/main/java/com/anmandu/connector/MySpringBootRouter.java)
picks each message up asynchronously and forwards it to one of four averaging
processors:

| Route | Strategy | Aggregator |
|---|---|---|
| `direct:AvgProcessor` | Running average over all messages ever seen | [`AvgAggregator`](src/main/java/com/anmandu/connector/aggregators/AvgAggregator.java) |
| `direct:WindowedAvgProcessor` | Average over fixed windows of **10 messages** | [`WindowedAvgAggregator`](src/main/java/com/anmandu/connector/aggregators/WindowedAvgAggregator.java) |
| `direct:TimeWindowedAvgProcessor` | Average over rolling **4-second** time windows | [`TimedAvgAggregator`](src/main/java/com/anmandu/connector/aggregators/TimedAvgAggregator.java) |
| `direct:PartitionTimeWindowedAvgProcessor` | Same 4-second time-windowed average, computed **per sensor** | [`PartitionTimedAvgAggregator`](src/main/java/com/anmandu/connector/aggregators/PartitionTimedAvgAggregator.java) |

Only the partitioned time-windowed route is currently wired into the consumer; the
other three are available but commented out in `configure()`. All four aggregators
are registered as Camel beans via [`ExtraRegistry`](src/main/java/com/anmandu/connector/ExtraRegistry.java).
`PartitionTimedAvgAggregator` keeps one `TimedAvgAggregator` per sensor name, so each
sensor's window is tracked independently.

There are also two disabled batch routes (`autoStartup(false)`) that ingest a JSON
file, unmarshal it into a list of `Input` objects, and fan them out through the same
queue: [`data/input_data.json`](data/input_data.json) (named individuals) and
[`data/input_data_sensor.json`](data/input_data_sensor.json) (tagged by sensor name,
matching the live publisher routes).

## Flow diagram

[`docs/sensor-flow-diagram.html`](docs/sensor-flow-diagram.html) shows the current
configuration: the four sensor timers feeding `MockInput`, the shared queue and
consumer, and the active partition + time-window aggregation path. Open it in a
browser to explore it interactively.

## Tech stack

- Java 17
- Spring Boot (Actuator exposes `health`, `info`, and `camelroutes`)
- Apache Camel Spring Boot (`camel-spring-boot-starter`, `camel-jackson-starter`, `camel-stream-starter`)

## Running it

```bash
./mvnw spring-boot:run
```

Watch the logs for lines like:

```
[Closed Window] start: ... | processed items: ... | avg: ...
```

## Running tests

```bash
./mvnw clean test
```

> ⚠️ No test coverage currently exists for the routes or aggregators — worth adding.

## More information

Learn more about Apache Camel at https://camel.apache.org/
