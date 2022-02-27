package com.fndef.streams.core

import java.time.LocalDateTime

import com.fndef.streams.core
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventSpec extends AnyFlatSpec with Matchers {
  "An event" should "have a data event type and attributes" in {
    val street: EventAttribute = EventAttribute("street", "some street")
    val city: EventAttribute = EventAttribute("city", "Nice city")
    val country: EventAttribute = EventAttribute("country", "USA")
    val postalCode: EventAttribute = EventAttribute("postal code", 100201)
    val addr1: Event = core.Event(street, city, country, postalCode)

    assert(addr1.eventType == DataEventType, "Event type is not data event")
    assert(addr1.eventAttributes.size == 4, "Event has incorrect number of attributes")
  }

  "Events with the same attributes" should "be equal" in {
    val street1: EventAttribute = EventAttribute("street", "some street")
    val city1: EventAttribute = EventAttribute("city", "Nice city")
    val country1: EventAttribute = EventAttribute("country", "USA")
    val postalCode1: EventAttribute = EventAttribute("postal code", 100201)
    val addr1: Event = core.Event(street1, city1, country1, postalCode1)

    val street2: EventAttribute = EventAttribute("street", "some street")
    val city2: EventAttribute = EventAttribute("city", "Nice city")
    val country2: EventAttribute = EventAttribute("country", "USA")
    val postalCode2: EventAttribute = EventAttribute("postal code", 100201)
    val addr2: Event = core.Event(street2, city2, country2, postalCode2)

    assert(addr1.eventType == DataEventType, "Incorrect event type")
    assert(addr2.eventType == DataEventType, "Incorrect event type")
    assert(addr1 == addr2, "Events with same attributes should be equal")
    assert(addr1.hashCode() == addr2.hashCode(), "Events with same attributes should have the same hash code")
    assert(addr1.toString == addr2.toString, "Events with same attributes should have the same toString")
  }

  "Events with the different attributes" should "not be equal" in {
    val street1: EventAttribute = EventAttribute("street1", "some street1")
    val city1: EventAttribute = EventAttribute("city1", "Nice city1")
    val country1: EventAttribute = EventAttribute("country", "USA")
    val postalCode1: EventAttribute = EventAttribute("postal code", 100202)
    val addr1: Event = core.Event(street1, city1, country1, postalCode1)

    val street2: EventAttribute = EventAttribute("street2", "some street")
    val city2: EventAttribute = EventAttribute("city2", "Nice city")
    val country2: EventAttribute = EventAttribute("country", "USA")
    val postalCode2: EventAttribute = EventAttribute("postal code", 100201)
    val addr2: Event = core.Event(street2, city2, country2, postalCode2)

    assert(addr1.eventType == DataEventType, "Incorrect event type")
    assert(addr2.eventType == DataEventType, "Incorrect event type")
    assert(addr1 != addr2, "Events with different attributes should not be equal")
    assert(addr1.hashCode() != addr2.hashCode(), "Events with different attributes should not have the same hash code")
    assert(addr1.toString != addr2.toString, "Events with different attributes should not have the same toString")
  }

  "Events with null attributes" should "fail at creation" in {
    assertThrows[IllegalArgumentException](core.Event(null))
  }

  "Events with no attributes" should "fail at creation" in {
    assertThrows[IllegalArgumentException](Event())
  }

  "Events with one attribute" should "get created with one attribute" in {
    val name: EventAttribute = EventAttribute("name", "James")
    val event : Event = core.Event(name)
    assert(event.eventType == DataEventType, "Incorrect event type")
    assert(event.eventAttributes == Seq(name), "Events attributes not correct")
  }

  "Events with non-null and null attributes " should "fail creation for null attributes" in {
    val name: EventAttribute = EventAttribute("name", "James")
    assertThrows[IllegalArgumentException](core.Event(name, null))
  }

  "Event Internal" should "get created using Events" in {
    val name: EventAttribute = EventAttribute("name", "James")
    val country: EventAttribute = EventAttribute("country", "USA")
    val event : Event = core.Event(name, country)

    val eventInternal: EventInternal = EventInternal(event)

    assert(eventInternal.eventType == DataEventType, "Event type is incorrect")
    assert(eventInternal.containsAttribute(name.name), "Attribute is missing")
    assert(eventInternal.containsAttribute(country.name), "Attribute is missing")
    assert((eventInternal.attributeNames == Set(name.name, country.name)), "Attribute names are not correct")
    assert(eventInternal.getAttribute(name.name) == Some(name), "Incorrect attribute value")
    assert(eventInternal.getAttribute(country.name) == Some(country), "Incorrect attribute value")
    assert(eventInternal.getAttribute("missing") == None, "Attribute should be missing")
  }

  "Event Internal " should " get created using event attributes" in {
    val name: EventAttribute = EventAttribute("name", "James")
    val country: EventAttribute = EventAttribute("country", "USA")

    val eventInternal: EventInternal = EventInternal(LocalDateTime.now(), Seq(name, country))
    assert(eventInternal.eventType == DataEventType, "Event type is incorrect")
    assert(eventInternal.containsAttribute(name.name), "Attribute is missing")
    assert(eventInternal.containsAttribute(country.name), "Attribute is missing")
    assert((eventInternal.attributeNames == Set(name.name, country.name)), "Attribute names are not correct")
    assert(eventInternal.getAttribute(name.name) == Some(name), "Incorrect attribute value")
    assert(eventInternal.getAttribute(country.name) == Some(country), "Incorrect attribute value")
    assert(eventInternal.getAttribute("missing") == None, "Attribute should be missing")
  }

  "Event Internal " should " get created for config update event type" in {
    val host: EventAttribute = EventAttribute("host", "localhost")
    val port: EventAttribute = EventAttribute("port", 8090)

    val eventInternal: EventInternal = EventInternal(ConfigUpdateType, LocalDateTime.now(), Seq(host, port))
    assert(eventInternal.eventType == ConfigUpdateType, "Event type is incorrect")
    assert(eventInternal.containsAttribute(host.name), "Attribute is missing")
    assert(eventInternal.containsAttribute(port.name), "Attribute is missing")
    assert((eventInternal.attributeNames == Set(host.name, port.name)), "Attribute names are not correct")
    assert(eventInternal.getAttribute(host.name) == Some(host), "Incorrect attribute value")
    assert(eventInternal.getAttribute(port.name) == Some(port), "Incorrect attribute value")
    assert(eventInternal.getAttribute("missing") == None, "Attribute should be missing")
  }

  "Event Internal " should " get created for admin event type" in {
    val adminAction: EventAttribute = EventAttribute("action", "createAction")

    val eventInternal: EventInternal = EventInternal(AdminEventType, LocalDateTime.now(), Seq(adminAction))
    assert(eventInternal.eventType == AdminEventType, "Event type is incorrect")
    assert(eventInternal.containsAttribute(adminAction.name), "Attribute is missing")
    assert((eventInternal.attributeNames == Set(adminAction.name)), "Attribute names are not correct")
    assert(eventInternal.getAttribute(adminAction.name) == Some(adminAction), "Incorrect attribute value")
    assert(eventInternal.getAttribute("missing") == None, "Attribute should be missing")
  }

  "Event internal with same event type and attributes" should "be equal" in {
    val name1: EventAttribute = EventAttribute("name", "James")
    val country1: EventAttribute = EventAttribute("country", "USA")
    val eventInternal1: EventInternal = EventInternal(LocalDateTime.now(), Seq(name1, country1))

    val name2: EventAttribute = EventAttribute("name", "James")
    val country2: EventAttribute = EventAttribute("country", "USA")
    val eventInternal2: EventInternal = EventInternal(LocalDateTime.now(), Seq(name2, country2))

    assert(eventInternal1.eventType == DataEventType, "Incorrect event type")
    assert(eventInternal2.eventType == DataEventType, "Incorrect event type")
    assert(eventInternal1 == eventInternal2, "Events with same attributes should be equal")
    assert(eventInternal1.hashCode() == eventInternal2.hashCode(), "Events with same attributes should have the same hash code")
    assert(eventInternal1.toString == eventInternal2.toString, "Events with same attributes should have the same toString")
  }

  "Event internal with same event type but different attributes" should "not be equal" in {
    val name1: EventAttribute = EventAttribute("name1", "James")
    val country1: EventAttribute = EventAttribute("country1", "USA")
    val eventInternal1: EventInternal = EventInternal(LocalDateTime.now(), Seq(name1, country1))

    val name2: EventAttribute = EventAttribute("name2", "James")
    val country2: EventAttribute = EventAttribute("country2", "USA")
    val eventInternal2: EventInternal = EventInternal(LocalDateTime.now(), Seq(name2, country2))

    assert(eventInternal1.eventType == DataEventType, "Incorrect event type")
    assert(eventInternal2.eventType == DataEventType, "Incorrect event type")
    assert(eventInternal1 != eventInternal2, "Events with different attributes should not be equal")
    assert(eventInternal1.hashCode() != eventInternal2.hashCode(), "Events with different attributes should not have the same hash code")
    assert(eventInternal1.toString != eventInternal2.toString, "Events with different attributes should not have the same toString")
  }

  "Event internal with same attributes but different event types" should "not be equal" in {
    val name1: EventAttribute = EventAttribute("name", "James")
    val country1: EventAttribute = EventAttribute("country", "USA")
    val eventInternal1: EventInternal = EventInternal(DataEventType, LocalDateTime.now(), Seq(name1, country1))

    val name2: EventAttribute = EventAttribute("name", "James")
    val country2: EventAttribute = EventAttribute("country", "USA")
    val eventInternal2: EventInternal = EventInternal(ConfigUpdateType, LocalDateTime.now(), Seq(name2, country2))

    assert(eventInternal1.eventType == DataEventType, "Incorrect event type")
    assert(eventInternal2.eventType == ConfigUpdateType, "Incorrect event type")
    assert(eventInternal1 != eventInternal2, "Events different event types should not be equal")
    assert(eventInternal1.hashCode() != eventInternal2.hashCode(), "Events with different event types should not have same the hash code")
    assert(eventInternal1.toString != eventInternal2.toString, "Events with different event types should not have the same toString")
  }

  "Event internal with missing event type" should "not be created" in {
    val name1: EventAttribute = EventAttribute("name", "James")
    assertThrows[IllegalArgumentException](EventInternal(null, LocalDateTime.now(), Seq(name1)))
  }

  "Event internal with null event attributes" should "not be created" in {
    assertThrows[IllegalArgumentException](EventInternal(ConfigUpdateType, LocalDateTime.now(), null))
  }

  "Event internal with no event attributes" should "not be created" in {
    assertThrows[IllegalArgumentException](EventInternal(AdminEventType, LocalDateTime.now(), Seq()))
  }

  "Event internal with some non-null and some null event attributes" should "not be created" in {
    val name1: EventAttribute = EventAttribute("name", "James")
    assertThrows[IllegalArgumentException](EventInternal(DataEventType, LocalDateTime.now(), Seq(name1, null)))
  }

  "Event and event internal with the same attributes" should "not be equal" in {
    val name1: EventAttribute = EventAttribute("name", "James")
    val event: Event = core.Event(name1)
    val eventInternal: EventInternal = EventInternal(event)

    assert(event != eventInternal, "Event and event internal should not be equal")
  }
}