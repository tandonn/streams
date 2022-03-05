package com.fndef.streams

import java.time.{Duration, LocalDateTime}

import com.fndef.streams.core.{EventAttribute, EventInternal, PipelineSink}

object TestStub {
  def main(args: Array[String]): Unit = {

    println("ok...")

    val pipeline = create("teststream")
      .windowBy(windowOf(Duration.ofMillis(1000)))
      .select("id", "name", "dept", "count(name)", "max(id)")
      .literals(lit(5), lit("test").as("hello-test"))
      // .strictGrouping(true)
      .filter(filterOf("name").equalTo(lit("james")).or(filterOf("name").equalTo(lit("susan"))))
      .having(filterOf("count(name)").greaterThanEqual(lit(1)))
      .groupBy("name", "dept")
      .start

    pipeline.registerSink(new PipelineSink {
      override def process(data: EventInternal): Unit = {
        println(s"downstream :: received event : ${data}")
      }
    });

    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "1"), EventAttribute("id", 1), EventAttribute("name", "james"), EventAttribute("dept", "abc"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "2"), EventAttribute("id", 2), EventAttribute("name", "joe"), EventAttribute("dept", "xyz"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "3"), EventAttribute("id", 3), EventAttribute("name", "susan"), EventAttribute("dept", "abc"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "4"), EventAttribute("id", 4), EventAttribute("name", "james"), EventAttribute("dept", "klm"))))
    pipeline.process(EventInternal(LocalDateTime.now(), Seq(EventAttribute("seqNo", "5"), EventAttribute("id", 4), EventAttribute("name", "james"), EventAttribute("dept", "klm"))))


    Thread.sleep(7000)
    pipeline.shutdown
    Thread.sleep(1000)
    println(s"pipeline is now shutdown - ${!pipeline.isActive}")

  }
}
