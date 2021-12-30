package com.fndef.streams.core.function

import com.fndef.streams.core.common.{WindowOp, WindowSpec}
import com.fndef.streams.core.{EventInternal, ProcessingContext, StreamFunction, StreamSink}

class WindowFunction(val name: String, literalFn: EnrichLiteral, groupFn: GroupFunction, filterFn: FilterFunction, selectFn: SelectFunction, val toFn:ToFunction) extends StreamFunction with WindowOp {
  implicit val processingContext: ProcessingContext = ProcessingContext()

  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = Option(event)

  override def windowOp(event: EventInternal): EventInternal = processEvent(event)
}

object WindowFunction {
  import com.fndef.streams._
  def apply(windowSpec: WindowSpec, sinks: Seq[StreamSink]): WindowFunction = {
    val literalfn = new EnrichLiteral(s"${windowSpec.pipelineName}-literals", windowSpec.literals)
    val groupFn = new GroupFunction(s"${windowSpec.pipelineName}-groupings", windowSpec.groupings, windowSpec.aggregations)
    val filterFn = new FilterFunction(s"${windowSpec.pipelineName}-filters", windowSpec.filters)
    val selectFn = new SelectFunction(s"${windowSpec.pipelineName}-projections", windowSpec.projections)
    val toFn = ToFunction(s"${windowSpec.pipelineName}-to", sinks: _*)
    val windowFn: WindowFunction = new WindowFunction(s"${windowSpec.pipelineName}-window", literalfn, groupFn, filterFn, selectFn, toFn)

    functionChain(windowFn, literalfn, groupFn, filterFn, selectFn, toFn)

    windowFn
  }
}
