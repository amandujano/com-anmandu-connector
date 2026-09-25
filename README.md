# com-anmandu-connector

A small **Apache Camel + Spring Boot** connector that simulates a stream of sensor
readings and computes rolling averages over them using three different strategies.

## What it does

Every `timer.period` milliseconds (default `500`), [`MockInput`](src/main/java/com/anmandu/connector/MockInput.java)
generates a random measurement and pushes it into an internal `seda:myInternalQueue`.
A consumer route picks each message up asynchronously and forwards it to one of three
averaging processors defined in [`MySpringBootRouter`](src/main/java/com/anmandu/connector/MySpringBootRouter.java):

| Route | Strategy | Aggregator |
|---|---|---|
| `direct:AvgProcessor` | Running average over all messages ever seen | [`AvgAggregator`](src/main/java/com/anmandu/connector/aggregators/AvgAggregator.java) |
| `direct:WindowedAvgProcessor` | Average over fixed windows of **10 messages** | [`WindowedAvgAggregator`](src/main/java/com/anmandu/connector/aggregators/WindowedAvgAggregator.java) |
| `direct:TimeWindowedProcessor` | Average over rolling **4-second** time windows | [`TimedAvgAggregator`](src/main/java/com/anmandu/connector/aggregators/TimedAvgAggregator.java) |

Only the time-windowed route is currently wired into the consumer; the other two are
available but commented out in `configure()`. All three aggregators are registered as
Camel beans via [`ExtraRegistry`](src/main/java/com/anmandu/connector/ExtraRegistry.java).

There's also a disabled batch route (`autoStartup(false)`) that can ingest
[`data/input_data.json`](data/input_data.json), unmarshal it into a list of `Input`
objects, and fan them out through the same queue in parallel.

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
