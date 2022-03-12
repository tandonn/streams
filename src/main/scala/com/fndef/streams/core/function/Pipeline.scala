package com.fndef.streams.core.function

import com.fndef.streams.PipelineSpec
import com.fndef.streams.core.{EventInternal, PipelineSink, PipelineSource, Startable, StreamFunction, StreamSink}
import com.fndef.streams.core.store.{IndexedStreamStore, StreamStore}

trait Pipeline extends PipelineSource with PipelineSink with Startable

class WindowedPipeline(spec: PipelineSpec) extends Pipeline {
  val name: String = s"${spec.pipelineName}"
  private[this] val streamStore: StreamStore = createStreamStore(pipelineFunctions)

  override def startup: Boolean = {
    println("starting pipeline")
    streamStore.startup
  }

  override def shutdown: Boolean = {
    println(s"shutting down ${name}")
    streamStore.shutdown
  }

  override def isActive: Boolean = streamStore.isActive

  private def createStreamStore(pipelineFunctions: StreamSink): StreamStore = {
    new IndexedStreamStore(s"${spec.pipelineName}-store", spec.windowSpec, pipelineFunctions)
  }

  private def pipelineFunctions: StreamFunction = {
    registerPipelineFunctions(createFilter, createGrouping, createHavingFilter, createSort, createLimit, createSelect, createToFunction)
  }

  private def createGrouping: GroupFunction = {
    new GroupFunction(s"${spec.pipelineName}-grouping", spec)
  }

  private def createSelect: SelectFunction = {
    new SelectFunction(s"${spec.pipelineName}-select", spec)
  }

  private def createSort: SortFunction = {
    new SortFunction(s"${spec.pipelineName}-sort", spec)
  }

  private def createLimit: LimitFunction = {
    new LimitFunction(s"${spec.pipelineName}-limit", spec)
  }

  private def createHavingFilter: FilterFunction = {
    new FilterFunction(s"${spec.pipelineName}-having", spec.having)
  }

  private def createFilter: FilterFunction = {
    new FilterFunction(s"${spec.pipelineName}-filter", spec.filters)
  }

  private def createToFunction: ToFunction = {
    new ToFunction(s"${spec.pipelineName}-to", this)
  }

  private def registerPipelineFunctions(streamFn: StreamFunction*): StreamFunction = {
    def register(fn: Seq[StreamFunction]): StreamFunction = {
      fn match {
        case source +: sink +: tail =>
          source.registerSink(sink)
          register(sink +: tail)
          source
        case sink +: tail =>
          sink
      }
    }
    streamFn.foreach(s => println(s"input fn = ${s.name}"))
    val s = register(streamFn)
    println(s"root of sinks = ${s.name}")
    s
  }

  def process(event: EventInternal): Unit = streamStore.addEvent(event)
}