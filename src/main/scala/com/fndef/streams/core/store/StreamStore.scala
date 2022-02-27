package com.fndef.streams.core.store

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.BlockingQueue

import com.fndef.streams.WindowSpec
import com.fndef.streams.core.{BatchSucess, EventBatch, EventInternal, Startable, StoreShutdown, StreamSink}
import com.fndef.streams.core.Implicits._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait StreamStore extends Startable {
  val id: String
  def addEvent(event: EventInternal): Boolean
}

trait EventTimeResolver {
  def eventTime(event: EventInternal): LocalDateTime
}

object EventTimeResolver {
  def apply(windowSpec: WindowSpec): EventTimeResolver = {
    if (windowSpec.internalTime) {
      new PipelineTime
    } else {
      new AttributeTimeString(windowSpec.timeAttribute, windowSpec.timeFormat)
    }
  }
}

class PipelineTime extends EventTimeResolver {
  override def eventTime(event: EventInternal): LocalDateTime = event.eventTime
}

class AttributeTimeMillis(timeAttr: String, missingHandler: EventInternal => LocalDateTime = e => throw new IllegalArgumentException("event time not found")) extends EventTimeResolver {
  override def eventTime(event: EventInternal): LocalDateTime = {
    event.getAttribute(timeAttr)
      .map(_.valueAs[Long])
      .map(millis => LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()))
      .getOrElse(missingHandler(event))
  }
}

class AttributeTimeString(eventTimeAttr: String, timeFormat: DateTimeFormatter, missingHandler: EventInternal => LocalDateTime = e => throw new IllegalArgumentException("event time not resolved")) extends EventTimeResolver {
  override def eventTime(event: EventInternal): LocalDateTime = {
    event.getAttribute(eventTimeAttr).map(_.valueAs[String])
      .map(s => LocalDateTime.parse(s))
      .getOrElse(missingHandler(event))
  }
}

case class Frame(startTime: LocalDateTime, endTime: LocalDateTime, complete: Boolean) extends Ordered[Frame] {
  override def compare(that: Frame): Int = {
    startTime.compareTo(that.startTime)
  }
}

class StreamFrames(windowSpec: WindowSpec) {
  class FrameIndexEntry(var frame: Frame)

  var frames: Seq[FrameIndexEntry] = initFrames()
  var dirtyFrameQueue: mutable.Queue[FrameIndexEntry] = mutable.Queue()

  private def initFrames(): Seq[FrameIndexEntry] = {
    val startTime = LocalDateTime.now()
    new FrameIndexEntry(Frame(startTime, startTime.plusNanos(windowSpec.windowDuration.toNanos), false)) +: Vector()
  }

  def markFramesComplete(now: LocalDateTime): Unit = {
    println(s" marking  frames complete count = ${frames.filterNot(_.frame.complete).filter(fe => now.isAfter(fe.frame.endTime)).size}")
    frames.filterNot(_.frame.complete).filter(fe => now.isAfter(fe.frame.endTime)).foreach(fe => fe.frame = Frame(fe.frame.startTime, fe.frame.endTime, true))
  }

  def markFrameDirty(eventTime: LocalDateTime): Boolean = {
    println("marking frames dirty")
    frames.find(fe => eventTime.isAfter(fe.frame.startTime) && eventTime.isBefore(fe.frame.endTime)) match {
      case Some(fe) if !dirtyFrameQueue.contains(fe) =>
        println(s"Mark and enqueue into dirty frames :: ${fe.frame}")
        dirtyFrameQueue.enqueue(fe)
        true
      case Some(_) =>
        println(s"already marked dirty :: dirty size = ${dirtyFrameQueue.size}")
        true
      case None =>
        println("none matched - not marking dirty")
        false
    }
  }

  def collectDirtyFrames: Seq[Frame] = {
    val f = dirtyFrameQueue.dequeueAll(_.frame.complete).map(_.frame).sorted
    println(s"dirty frames to process = ${f.size}")
    f
  }

  def createPendingFrames(now: LocalDateTime): Unit = {
    def create(currTime: LocalDateTime, startTime: LocalDateTime, endTime: LocalDateTime, entries: Seq[FrameIndexEntry]): Seq[FrameIndexEntry] = {
      val nextStart = startTime.plusNanos(windowSpec.slideBy.toNanos)
      if (currTime.isAfter(nextStart)) {
        val nextEnd = nextStart.plusNanos(windowSpec.windowDuration.toNanos)
        create(currTime, nextStart, nextEnd, new FrameIndexEntry(Frame(nextStart, nextEnd, false)) +: entries)
      } else {
        entries
      }
    }

    val startTime = frames(0).frame.startTime
    val endTime = startTime.plusNanos(windowSpec.windowDuration.toNanos)
    frames = create(now, startTime, endTime, Vector()) ++ frames
    //
    println(s"No of frames = ${frames.size}")
    frames.foreach(fe => println(fe.frame))
    //
  }

  def removeExpiredFrames(now: LocalDateTime): Unit = {
    println(s"expiry time = ${windowSpec.expireBy.toMillis} - ${windowSpec.expireBy.getUnits} :: now = ${now} :: expiry = ${now.minusNanos(windowSpec.expireBy.toNanos)}")
    println(s"expired frames to be removed = ${frames.filter(fe => fe.frame.startTime.plusNanos(windowSpec.expireBy.toNanos).isBefore(now))}")
    frames = frames.filterNot(fe => fe.frame.startTime.plus(windowSpec.expireBy).isBefore(now))
    println(s"frame count now - ${frames.size}")
  }
}


class StreamStoreTask(pendingQueue: BlockingQueue[EventInternal], eventStream: EventStream, frames: StreamFrames, startSink: StreamSink, windowSpec: WindowSpec) extends Runnable {
  override def run(): Unit = {
    println("running task")
    val now: LocalDateTime = LocalDateTime.now()
    frames.createPendingFrames(now)
    frames.markFramesComplete(now)
    storeEvents
    processFrames(frames.collectDirtyFrames, now)
    frames.removeExpiredFrames(now)
    eventStream.slideTo(now.minus(windowSpec.expireBy))
  }

  private def processFrames(frames: Seq[Frame], now: LocalDateTime): Unit = {
    //
    println(s"Dirty frames to process and publish = ${frames.size}")
    frames.foreach(f => println(f))
    //
    frames match {
      case frame +: pending =>
        println(s"publishing frame to processing select/filter pipeline :: ${frame}")
        println(s"No of events in frame = ${eventStream.slice(frame.startTime, frame.endTime).reverse.size}")
        startSink.process(EventBatch(LocalDateTime.now(), eventStream.slice(frame.startTime, frame.endTime).map(_._2).reverse))

        println("process next frame")
        processFrames(pending, now)
      case Seq() =>
        println("all frames processed")

    }
  }

  @tailrec
  private def storeEvents: Unit = {
    println(s"Current pending queue size = ${pendingQueue.size()}")
    Option(pendingQueue.poll()) match {
      case Some(event) =>
        println(s"inserting event into stream = $event")
        Try {
          val entry = eventStream.insert(event)
          frames.markFrameDirty(entry._1)
        } match {
          case Success(_) =>
          case Failure(ex) =>
            ex.printStackTrace()
        }
        println("fetch next event from pending event queue")
        storeEvents
      case None =>
        println("No event found in pending event queue")
    }
  }
}

class StreamStoreRunner(name: String, windowSpec: WindowSpec, streamTask: StreamStoreTask, taskStatusSink: Option[StreamSink] = None) extends Thread(name) with Startable {

  private[this] val lock = new ReentrantLock()
  @volatile private[this] var active = true;

  override def run(): Unit = {
    @tailrec
    def runLoop(loopActive: Boolean): Boolean = {
      loopActive match {
        case true =>
          Try {
            println(s"starting to run task :: window duration = ${windowSpec.windowDuration.toMillis} :: window slide by = ${windowSpec.slideBy.toMillis}")
            streamTask.run()
            Thread.sleep(windowSpec.slideBy.toMillis)
          } match {
            case Success(_) => taskStatusSink.foreach(_.process(BatchSucess(LocalDateTime.now())))
            case Failure(exception) => exception.printStackTrace()
            // case Failure(exception) => taskStatusSink.foreach(_.process(BatchError(LocalDateTime.now(), exception.getMessage, exception)))
          }
          runLoop(active)
        case false =>
          false
      }
    }
    runLoop(isActive)
  }

  override def startup: Boolean = {
    try {
      lock.lockInterruptibly()
      if (active) {
        println("starting store runner")
        start()
      }
    } finally {
      lock.unlock()
    }
    println(s"started store runner - ${active}")
    active
  }

  override def shutdown: Boolean = {
    try {
      lock.lockInterruptibly()
      active = false
      taskStatusSink.foreach(_.process(StoreShutdown(LocalDateTime.now, name)))
    } finally {
      lock.unlock()
    }
    active
  }

  override def isActive: Boolean = active
}