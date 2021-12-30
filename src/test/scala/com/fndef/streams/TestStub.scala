package com.fndef.streams

import com.fndef.streams.core.common.{AndFilter, AttributeOp, LessThanEqual, NotEqualFilter}
import com.fndef.streams.core.{EventAttribute, EventInternal}

object TestStub {
  def main(args: Array[String]): Unit = {

    println("ok...")

    val pipeline = create("teststream")
      .select("a2", "a3", "max(a3)", "count(a1)", "sum(a2)", "max(a2)", "min(a2)", "avg(a2)")
      .filter(AndFilter(LessThanEqual("count(a1)", "lit(1)"), NotEqualFilter("a1", "lit(a1val1)"))) // lit() does literal value comparision
      .groupBy("a1")
      .start

    import com.fndef.streams.core.Implicits._
    pipeline.processEvent(EventInternal(Seq(EventAttribute("a1", "a1val1"), EventAttribute("a2", 7.3), EventAttribute("a3", "a33"))))
    pipeline.processEvent(EventInternal(Seq(EventAttribute("a1", "a1val2"), EventAttribute("a2", 3.2), EventAttribute("a3", "a32"))))
    pipeline.processEvent(EventInternal(Seq(EventAttribute("a1", "a1val1"), EventAttribute("a2", 7.2), EventAttribute("a3", "a31"))))
  }
}
