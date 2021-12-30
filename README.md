# Streams
Streams is a stream processing framework. This is WIP

## Key features
Streams supports building pipelines that can be plugged together to build complex data streams. Each individual pipeline can perform simple enrich, transform and aggregation operations and can then publish the results to the next pipeline for further processing.
Piplines support pluggable functions for custom transformations and data enrichments.

Following are the key features:
* Create a pipeline
* Publish data into the pipeline
* Custom data events which hold a collection of attributes
* Filter, aggregate and select specific set of attributes
* Publish the results of one pipeline into next for further processing
* Support for tumbling and sliding windows for data operations

## How to build a pipeline

Create a pipline that:
* Groups events by name and country
* Apply filter criteria and select those events which do not have name "John" and where count of names is less than equal to 100
* Select the name, age, salary and country from the event
* For the events matching filter criteria, calculate and select the aggregates
    * count(name)
    * max(age)
    * sum(salary)
    * max(salary)
    * min(salary)
    * avg(salary)

```    
val pipeline = create("stream_name")
      .select("name", "age", "salary", "country", "some_other_col", "count(name)", "max(age)", "sum(salary)", "max(salary)", "min(salary)", "avg(salary)")
      .filter(AndFilter(LessThanEqual("count(name)", "lit(100)"), NotEqualFilter("name", "lit(John)"))) // lit() does literal value comparision
      .groupBy("name", "country")
      .start
```

## Road map
* Pluggable data transformation functions that allow plugging custom logic to be executed with the pipeline
* Custom filter and aggregation functions
* Custom joins that combine events based on configurable criteria
* Distributed streams which allow pipelines to run across nodes
* Partitioned windows data that allow large windows to spread data across nodes