package com.fndef.streams.core.store

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.BlockingQueue

import com.fndef.streams.WindowSpec
import com.fndef.streams.core.{BatchError, BatchSucess, EventBatch, EventInternal, Startable, StoreShutdown, StreamSink}
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

  private[this] val lock = new ReentrantLock()
  private[this] var frames: Seq[FrameIndexEntry] = initFrames()
  private[this] val dirtyFrameQueue: mutable.Queue[FrameIndexEntry] = mutable.Queue()

  private def initFrames(): Seq[FrameIndexEntry] = {
    val startTime = LocalDateTime.now()
    new FrameIndexEntry(Frame(startTime, startTime.plusNanos(windowSpec.windowDuration.toNanos), false)) +: Vector()
  }

  def markFramesComplete(now: LocalDateTime): Unit = {
    try {
      lock.lockInterruptibly()
      println(s" marking  frames complete count = ${frames.filterNot(_.frame.complete).filter(fe => now.isAfter(fe.frame.endTime)).size}")
      frames.filterNot(_.frame.complete).filter(fe => now.isAfter(fe.frame.endTime)).foreach(fe => fe.frame = Frame(fe.frame.startTime, fe.frame.endTime, true))
    } finally {
      lock.unlock()
    }
  }

  def markFrameDirty(eventTime: LocalDateTime): Boolean = {
    try {
      lock.lockInterruptibly()
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
    } finally {
      lock.unlock()
    }
  }

  def collectDirtyFrames: Seq[Frame] = {
    try {
      lock.lockInterruptibly()
      val f = dirtyFrameQueue.dequeueAll(_.frame.complete).map(_.frame).sorted
      println(s"dirty frames to process = ${f.size}")
      f
    } finally {
      lock.unlock()
    }
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

    try {
      lock.lockInterruptibly()
      val currFrame = frames(0).frame
      frames = create(now, currFrame.startTime, currFrame.endTime, Vector()) ++ frames
      //
      println(s"No of frames = ${frames.size}")
      frames.foreach(fe => println(fe.frame))
      //
    } finally {
      lock.unlock()
    }
  }

  def removeExpiredFrames(now: LocalDateTime): Unit = {
    try {
      lock.lockInterruptibly()
      println(s"expired frames to be removed = ${frames.filter(fe => fe.frame.startTime.isBefore(now)).map(_.frame)}")
      frames = frames.filterNot(fe => fe.frame.startTime.isBefore(now))
      println(s"frame count now - ${frames.size}")
    } finally {
      lock.unlock()
    }
  }
}


class StreamStoreTask(eventStream: WindowedEventStream, startSink: StreamSink, windowSpec: WindowSpec) extends Runnable {
  override def run(): Unit = {
    println("running task")
    processFrames(eventStream.dequeueDirtyFrames)
    eventStream.slideTo(LocalDateTime.now().minus(windowSpec.expireBy))
    println("task complete")
  }

  private def processFrames(frames: Seq[Frame]): Unit = {
    def publishFrame(frame: Frame): Frame = {
      println(s"publishing frame to processing select/filter pipeline :: ${frame}")
      println(s"No of events in frame = ${eventStream.slice(frame.startTime, frame.endTime).size}")
      Try {
        startSink.process(EventBatch(LocalDateTime.now(), eventStream.slice(frame.startTime, frame.endTime).map(_._2)))
      } match {
        case Success(_) =>
          println(s"published frame : ${frame}")
        case Failure(exception) =>
          println(s"failed to process batch for frame : ${frame}")
          exception.printStackTrace()
      }
      frame
    }

    //
    println(s"Dirty frames to process and publish = ${frames.size}")
    frames.foreach(f => println(f))
    //
    frames.foreach(publishFrame(_))
  }

  /*
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

   */
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
            case Failure(exception) =>
              exception.printStackTrace()
              taskStatusSink.foreach(_.process(BatchError(LocalDateTime.now(), exception.getMessage, exception)))
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
      println(s"started store runner - ${active}")
      active
    } finally {
      lock.unlock()
    }
  }

  override def shutdown: Boolean = {
    try {
      lock.lockInterruptibly()
      active = false
      taskStatusSink.foreach(_.process(StoreShutdown(LocalDateTime.now, name)))
      active
    } finally {
      lock.unlock()
    }
  }

  override def isActive: Boolean = {
    try {
      lock.lockInterruptibly()
      active
    } finally {
      lock.unlock()
    }
  }
}