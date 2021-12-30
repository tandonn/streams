package com.fndef.streams.core

sealed trait EventType
case object DataEventType extends EventType
case object ConfigUpdateType extends EventType
case object AdminEventType extends EventType

final class Event(val eventAttributes: Seq[EventAttribute]) {
  require(eventAttributes.nonEmpty && eventAttributes.count(_ == null) == 0, "Event has missing or null attributes")

  final val eventType: EventType = DataEventType

  override def equals(obj: Any): Boolean = {
    obj match {
      case x if x.isInstanceOf[Event] =>
        x.asInstanceOf[Event].eventAttributes.sameElements(eventAttributes)
      case _ => false
    }
  }

  override def hashCode(): Int = eventAttributes.hashCode()

  override def toString: String = s"Event[eventType=${eventType}, attributes={${eventAttributes.mkString(",")}}]"
}

object Event {
  def apply(eventAttributes: EventAttribute*): Event = {
    new Event(eventAttributes)
  }
}

protected[core] class EventInternal(val eventType: EventType, private val eventAttributes: Map[String, EventAttribute]) {
  require(Option(eventType).nonEmpty, "Event type is required")
  require(Option(eventAttributes).nonEmpty && eventAttributes.nonEmpty && eventAttributes.count(_ == null) == 0, "Event attributes are required and should not be null")

  def attributeNames: Set[String] = eventAttributes.keySet

  def attributes: Seq[EventAttribute] = eventAttributes.map(_._2).toList

  def getAttribute(name: String): Option[EventAttribute] = {
    require(name != null, "Attribute name is null")
    eventAttributes.get(name)
  }

  def containsAttribute(name: String): Boolean = {
    eventAttributes.contains(name)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case x if x.isInstanceOf[EventInternal] =>
        val e = x.asInstanceOf[EventInternal]
        e.eventType == eventType && e.eventAttributes == eventAttributes
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    (eventType.hashCode() + eventAttributes.map(_._2).toSeq.hashCode()).toInt
  }

  override def toString: String = {
    s"EventInternal[eventType=${eventType}, eventAttributes={${eventAttributes.map(_._2).mkString(",")}}]"
  }
}

object EventInternal {
  def apply(event: Event): EventInternal = new EventInternal(event.eventType, event.eventAttributes.map(e => (e.name, e)).toMap)

  def apply(eventType: EventType, eventAttributes: Seq[EventAttribute]): EventInternal = {
    require(Option(eventAttributes).nonEmpty && eventAttributes.nonEmpty && eventAttributes.count(_ == null) == 0, "Event attributes are required and should not be null")
    new EventInternal(eventType, eventAttributes.map(e => (e.name, e)).toMap)
  }

  def apply(eventAttributes: Seq[EventAttribute]): EventInternal = {
    require(Option(eventAttributes).nonEmpty && eventAttributes.nonEmpty && eventAttributes.count(_ == null) == 0, "Event attributes are required and should not be null")
    new EventInternal(DataEventType, eventAttributes.map(e => (e.name, e)).toMap)
  }
}