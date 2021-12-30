package com.fndef

import com.fndef.streams.core.{StreamFunction, StreamSink, StreamSource}
import com.fndef.streams.core.common.{AggregateOp, AttributeOp, ContinousWindow, FilterOp, LiteralOp, WindowSpec, WindowType}
import com.fndef.streams.core.function.Pipeline

import scala.collection.mutable

package object streams {
  type Event = com.fndef.streams.core.Event

  class PipelineBuilder(pipelineName: String, windowStrategy: WindowType) {
    val selectCriteria: mutable.Set[AttributeOp] = mutable.Set()
    val filterCriteria: mutable.Set[FilterOp] = mutable.Set()
    val literalCriteria: mutable.Set[AttributeOp] = mutable.Set()
    val groupCriteria: mutable.Set[AttributeOp] = mutable.Set()
    val aggregateCriteria: mutable.Set[AttributeOp] = mutable.Set()

    def this(piplineName: String) = this(piplineName, new ContinousWindow)
    def select(attributes: String*): PipelineBuilder = {
      val selectAttributes = attributes.map(AttributeOp(_))
      selectCriteria ++= selectAttributes
      literalCriteria ++= selectAttributes.filter(a => a.isInstanceOf[LiteralOp])
      aggregateCriteria ++= selectAttributes.filter(a => a.isInstanceOf[AggregateOp])
      this
    }

    def filter(filterAttributes: FilterOp*): PipelineBuilder = {
      filterCriteria ++= filterAttributes
      this
    }

    def groupBy(attributes: String*): PipelineBuilder = {
      groupCriteria ++= attributes.map(AttributeOp(_))
      this
    }

    def start: Pipeline = {
      new Pipeline(pipelineName, windowStrategy, windowSpec)
    }

    private def windowSpec: WindowSpec = {
      WindowSpec(pipelineName, literalCriteria.toSeq, groupCriteria.toSeq, aggregateCriteria.toSeq, filterCriteria.toSeq, selectCriteria.toSeq)
    }
  }

  def create(streamName: String): PipelineBuilder = {
    new PipelineBuilder(streamName, new ContinousWindow)
  }

  def create(streamName: String, windowStrategy: WindowType): PipelineBuilder = {
    new PipelineBuilder(streamName, windowStrategy)
  }

  def registerSinks[T <: StreamSource](source: T)(sinks: StreamSink*): T = {
    sinks.foreach(source.registerSink(_))
    source
  }

  def functionChain(functions: StreamFunction*): Seq[StreamFunction] = {
    functions match {
      case head +: next +: tail =>
        head.registerSink(next)
        head +: functionChain((next +: tail): _*)
      case head +: tail =>
        Seq(head)
      case Nil => Nil
    }
  }
}
