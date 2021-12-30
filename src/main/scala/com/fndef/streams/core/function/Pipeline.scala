package com.fndef.streams.core.function

import com.fndef.streams.core._
import com.fndef.streams.core.common.{Error, Expired, Ready, WindowSpec, WindowType}

class Pipeline(val name: String, windowType: WindowType, windowSpec: WindowSpec) extends StreamFunction {
  var windowFunction: WindowFunction = WindowFunction(windowSpec, sinks)

  override def processEvent(event: EventInternal)(implicit context: ProcessingContext): EventInternal = {
    windowFunction = windowType.winState match {
      case Ready =>
        windowFunction
      case Expired | Error =>
        windowFunction.toFn.clearSinks
        WindowFunction(windowSpec, sinks)
    }
    windowFunction.windowOp(event)
  }

  override def registerSink(streamSink: StreamSink): StreamSource = {
    windowFunction.toFn.registerSink(streamSink)
    super.registerSink(streamSink)
  }

  override def deregisterSink(streamSink: StreamSink): StreamSource = {
    windowFunction.toFn.deregisterSink(streamSink)
    super.deregisterSink(streamSink)
  }
}