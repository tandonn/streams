# Streams
Streams is a stream processing framework. This is WIP

## Key features
Streams supports building pipelines that can be plugged together to build complex data streams. Each individual pipeline can perform simple enrich, transform and aggregation operations and can then publish the results to the next pipeline for further processing.
Piplines support pluggable functions for custom transformations and data enrichments.

Following are the key features:
* Create a windowed streaming event pipeline using SQL like filter, aggregate, and select functions
* API to publish data into the pipeline
* Custom data events which hold a collection of attributes
* Filter, aggregate and select specific set of attributes
* Publish the results of one pipeline into next for further processing. Join multiple streams using custom criteria to create complex streams
* Support for tumbling and sliding windows for data operations

## How to build a pipeline

Create a pipline that:
* Filters event where name is james or name is susan
* Filters are applied before any data processing. In distributed streams, these will be pushed up to the source so that only the filtered events are published to downstream sinks
* Groups events (having name = james or susan ) by name and dept. Grouping can apply group functions like count, min,max, avg etc
* Apply having criteria which is filter criteria applied after grouping. For example if the criteria is count(name) > 5, all events will be pushed into the stream and grouped, and only once the grouped criteria of count(name) > 5 is met the events are published downstream 
* Select specific attributes from the attribute set for each selected event like id, name, dept, count(name),max(id), or use "*" to select all attributes
* For the events matching filter criteria, calculate and select the aggregates
    * count(name)
    * max(age)
    * sum(salary)
    * max(salary)
    * min(salary)
    * avg(salary)

### Create a pipeline
```
    val pipeline = create("teststream")
      .windowBy(windowOf(Duration.ofMillis(1000)))
      .select("id", "name", "dept", "count(name)", "max(id)")
      .filter( filterOf("name").equalTo( lit("james") ).or( filterOf("name").equalTo( lit("susan") )))
      .having( filterOf("count(name)").greaterThanEqual( lit(1) ))
      .groupBy("name", "dept")
      .start
```

### Register custom sinks
Custom sinks can be registered for listening to events. Sinks can be other pipelines that receive events from upstream and do further processing (filter/aggergate/transform operations). Join sinks support joining data from multiple upstreams in complex sql like structures

API for distributed streams is WIP. To listen to events from a local stream register as a sink as shown below.

```
    pipeline.registerSink(new PipelineSink {
      override def process(data: EventInternal): Unit = {
        println(s"downstream :: received event : ${data}")
      }
    });
```

### Publish events into the pipeline
The pipeline will support an api to publish into the distributed streams. This is WIP

You can also publish directly into a local pipeline as shown below.

```
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "1"), EventAttribute("id", 1), EventAttribute("name", "james"), EventAttribute("dept", "abc"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "2"), EventAttribute("id", 2), EventAttribute("name", "joe"), EventAttribute("dept", "xyz"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "3"), EventAttribute("id", 3), EventAttribute("name", "susan"), EventAttribute("dept", "abc"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "4"), EventAttribute("id", 4), EventAttribute("name", "james"), EventAttribute("dept", "klm"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "5"), EventAttribute("id", 4), EventAttribute("name", "james"), EventAttribute("dept", "klm"))))

```
## Road map
* Pluggable data transformation functions that allow plugging custom logic to be executed with the pipeline
* Custom filter, aggregation and tranformation functions that can be applied to each pipeline
* Custom joins across streams that combine events based on configurable criteria similar to complex SQL joins
* Distributed streams which allow pipelines to run across nodes
* Partitioned windows data that allow large windows to spread data across nodes