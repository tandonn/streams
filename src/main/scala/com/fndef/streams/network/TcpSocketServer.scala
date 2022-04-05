package com.fndef.streams.network

import java.net.InetSocketAddress
import java.nio.channels.{Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import com.fndef.streams.core.{Pipe, Startable, WithResponsePipe}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait ServerType

abstract class TcpSocketServer[T](val id: String, val port: Int, val serverType: ServerType, val maxRestartAttempts: Int = 3, val description: String = "") extends WithResponsePipe[T] with Startable {
  require(port > 0, s"Invalid server port - ${port}")

  private[this] val lock = new ReentrantLock()
  private[this] val active: AtomicBoolean = new AtomicBoolean(false);
  private[this] val pendingRestartAttempts = new AtomicInteger(maxRestartAttempts)
  private[this] var serverLoop = new ServerLoop[T](this)

  private[network] def createEndpoint(channel: SocketChannel): ChannelEndpoint
  private[network] def createServerReadHandler: ServerReadHandler
  private[network] def createServerWriteHandler: ServerWriteHandler[T]

  override def shutdown: Boolean = {
    try {
      lock.lockInterruptibly()
      println(s"Shutting down server - ${id}")
      active.set(false)
      serverLoop.shutdown
    } finally {
      lock.unlock()
    }
  }

  override def isActive: Boolean = {
    try {
      lock.lockInterruptibly()
      active.get() && serverLoop.isActive
    } finally {
      lock.unlock()
    }
  }

  override def startup: Boolean = {
    require(!serverLoop.isActive, s"server [${id}] is already running on port[${port}]")
    require(pendingRestartAttempts.getAndDecrement() > 0, s"Server [${id}] on port [${port}] has exhausted startup attempts")
    try {
      lock.lockInterruptibly()
      serverLoop = new ServerLoop(this)
      val started = serverLoop.startup
      active.set(started)
      started
    } finally {
      lock.unlock()
    }
  }

  def responsePipe: Pipe[T] = {
    serverLoop.responseListener
  }
}

private class ResponseListener[T](val id: String, writeHandler: ServerWriteHandler[T]) extends Pipe[T] {
  override def process(response: T): Unit = writeHandler.enqueue(response)
}

private class ServerLoop[T](tcpServer: TcpSocketServer[T]) extends ServerThread(tcpServer.id) {
  private[this] val latch = new CountDownLatch(1)
  private[this] val active: AtomicBoolean = new AtomicBoolean(false)
  private[this] val stopped: AtomicBoolean = new AtomicBoolean(false)
  private[this] val readHandler: ServerReadHandler = tcpServer.createServerReadHandler
  private[this] val writeHandler: ServerWriteHandler[T] = tcpServer.createServerWriteHandler
  private[this] val restartHandler = new ServerRestartHandler[T](tcpServer, 1)
  private[this] lazy val selector: Selector = Selector.open()
  private[this] lazy val serverChannel: ServerSocketChannel = ServerSocketChannel.open()

  private[network] val responseListener: ResponseListener[T] = new ResponseListener[T](s"${tcpServer.id}-response-listener", writeHandler)

  override def run(): Unit = {
    @tailrec
    def runLoop(server: ServerSocketChannel, selector: Selector): Unit = {
      if (active.get()) {
        println(s"server [${id}] waiting for connections on port [${tcpServer.port}]")
        val socketChannel: SocketChannel = server.accept();
        println("new socket connection")
        val endpoint = tcpServer.createEndpoint(socketChannel)
        readHandler.registerChannel(EndpointTarget(endpoint, ReadEndpoint))
        writeHandler.registerChannel(EndpointTarget(endpoint, WriteEndpoint))
        runLoop(server, selector)
      } else {
        println(s"server loop for instance [${id}] running on port [${tcpServer.port}] is no longer active")
        shutdown
      }
    }

    try {
      active.set(true)
      readHandler.startup
      writeHandler.startup
      latch.countDown()
      serverChannel.socket().bind(new InetSocketAddress(tcpServer.port))
      println(s"started server [${id}] loop on port [${tcpServer.port}]")
      runLoop(serverChannel, selector)
    } finally {
      active.set(false)
      stopped.set(true)
      println(s"server instance [${id}] on port [${tcpServer.port}] is shutdown")
    }
  }

  override def startup: Boolean = {
    require(!stopped.get(), s"this instance of server [${id}] on port [${tcpServer.port}] has been stopped. Restart of the same instance is not supported")
    require(active.compareAndSet(false, true), s"server [${id}] already running on port [${tcpServer.port}]")
    println(s"initializing server [${id}] on port [${tcpServer.port}]")
    setUncaughtExceptionHandler(restartHandler)
    readHandler.setUncaughtExceptionHandler(restartHandler)
    writeHandler.setUncaughtExceptionHandler(restartHandler)
    start()
    latch.await()
    isActive
  }

  override def shutdown: Boolean = {
    try {
      active.set(false)
      stopped.set(true)
      Try(serverChannel.close())
      Try(writeHandler.shutdown)
      Try(readHandler.shutdown)
    } catch {
      case e: Exception =>
        println(s"shutdown not clean - ${e.getMessage}")
    } finally {
      Try(selector.close())
    }
    !active.get() && stopped.get()
  }

  override def isActive: Boolean = active.get() && !stopped.get()
}

class ServerRestartHandler[T](server: TcpSocketServer[T], restartAttempts: Int) extends Thread.UncaughtExceptionHandler {

  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    println(s"uncaught exception in server [${server.id}] - ${e.getMessage}")
    e.printStackTrace()
    // attempt recycle of server loop in case of a fatal or unhandled exception
    try {
      if (server.isActive) {
        val isStarted = restart(restartAttempts)
        println(s"Server on port [${server.port}] - restart successful - [${isStarted}]")
      }
    }
  }

  private def restart(maxAttempts: Int): Boolean = {
    @tailrec
    def attemptStart(serverStarted: Boolean, attempt: Int): Boolean = {
      println(s"Attempting restart of server on port [${server.port}]")
      if (!serverStarted && attempt < maxAttempts) {
        Try(server.shutdown) match {
          case Success(_) =>
            println(s"Stopped server on port [${server.port}]. Now restarting...")
          case Failure(exception) =>
            exception.printStackTrace()
            println(s"Failed clean shutdown of server on port [${server.port}]")
        }
        attemptStart(server.startup, attempt + 1)
      } else serverStarted
    }

    attemptStart(false, 0)
  }
}