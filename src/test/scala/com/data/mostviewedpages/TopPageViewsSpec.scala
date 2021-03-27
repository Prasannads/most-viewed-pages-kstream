package com.data.mostviewedpages

import java.util.concurrent.TimeUnit

import TopPageViews.buildGlobalKTable
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import ksql._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import scala.collection.JavaConverters._

class TopPageViewsSpec
    extends AnyFunSpec
    with BeforeAndAfter
    with MainSpec
    with PrivateMethodTester {

  var topologyDriver: TopologyTestDriver                = _
  var usersTopic: TestInputTopic[String, users]         = _
  var pageViewsTopic: TestInputTopic[String, pageviews] = _
  var topPagesTopic: TestOutputTopic[String, toppages]  = _

  before {

    implicit val stringSerde: Serde[String]           = null
    implicit val usersSerde: SpecificAvroSerde[users] = null

    registerTopicSchemaInDummySchemaRegistry()

    val builder: StreamsBuilder = initTestTopology

    val usersGlobalKTable =
      buildGlobalKTable[String, users](builder, testUsersTopicName)

    val invokeTransformations = PrivateMethod[Unit]('transformations)

    TopPageViews invokePrivate invokeTransformations(
      builder,
      usersGlobalKTable,
      testPageViewsTopicName,
      testTopPagesTopicName
    )

    val testTopologyProperties = getTestTopologyProperties

    val testTopology = buildTestTopology(builder)

    topologyDriver = new TopologyTestDriver(testTopology, testTopologyProperties)

    // Create input topics
    usersTopic = createInputTopic[users](topologyDriver, testUsersTopicName, usersSerializer)

    pageViewsTopic =
      createInputTopic[pageviews](topologyDriver, testPageViewsTopicName, pageViewsSerializer)

    // Create output topic
    topPagesTopic =
      createOutputTopic[toppages](topologyDriver, testTopPagesTopicName, topPagesDeserializer)
  }

  after {
    topologyDriver.close()
  }

  private def setEventTime(mins: Long): Long = {
    System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(
      mins - 1 + 1
    )
  }

  private def createUsersEvent(userId: String, gender: String): users = {
    users
      .newBuilder()
      .setUserid(userId)
      .setGender(gender)
      .setRegionid("1")
      .setRegistertime(123L)
      .build()
  }

  private def createTopPagesEvent(gender: String,
                                  pageId: String,
                                  distinctUsers: Long,
                                  viewTime: Long): toppages = {
    toppages
      .newBuilder()
      .setGender(gender)
      .setPageid(pageId)
      .setDistinctusers(distinctUsers)
      .setViewtime(viewTime)
      .build()
  }

  private def createPageViewsEvent(pageId: String, userId: String, viewTime: Long): pageviews = {
    pageviews.newBuilder().setPageid(pageId).setUserid(userId).setViewtime(viewTime).build()
  }

  describe("top page views stream test") {
    pending
    it("should calculate sum view time based on gender and pageid") {

      usersTopic.pipeInput(
        "12345678",
        createUsersEvent("12345678", "MALE")
      )

      usersTopic.pipeInput(
        "12345679",
        createUsersEvent("12345679", "FEMALE")
      )

      usersTopic.pipeInput(
        "12345680",
        createUsersEvent("12345680", "OTHER")
      )

      pageViewsTopic.pipeInput(
        "12345678",
        pageviews.newBuilder().setPageid("page1").setUserid("12345678").setViewtime(1L).build(),
        setEventTime(1)
      )

      pageViewsTopic.pipeInput(
        "12345678",
        pageviews.newBuilder().setPageid("page1").setUserid("12345678").setViewtime(3L).build(),
        setEventTime(1)
      )

      pageViewsTopic.pipeInput(
        "12345679",
        pageviews.newBuilder().setPageid("page2").setUserid("12345679").setViewtime(1L).build(),
        setEventTime(1)
      )

      pageViewsTopic.pipeInput(
        "12345681",
        pageviews.newBuilder().setPageid("page3").setUserid("12345679").setViewtime(4L).build(),
        setEventTime(1)
      )

      assert(!topPagesTopic.isEmpty)
      assert(topPagesTopic.getQueueSize == 2)

      val actualTopPages = topPagesTopic.readValuesToList()
      val expectedTopPages = List(createTopPagesEvent("MALE", "page1", 1, 4L),
                                  createTopPagesEvent("FEMALE", "page2", 1, 1L),
                                  createTopPagesEvent("FEMALE", "page3", 1, 4L))

      assert(actualTopPages.asScala == expectedTopPages)
    }

    pending
    it("should discard event if there is no user in Users") {

      usersTopic.pipeInput(
        "12345678",
        createUsersEvent("12345678", "MALE")
      )

      pageViewsTopic.pipeInput(
        "12345679",
        pageviews.newBuilder().setPageid("page1").setUserid("12345679").setViewtime(1L).build(),
        setEventTime(1)
      )

      assert(topPagesTopic.isEmpty)
    }

    pending
    it("should emit only top 10 in a window") {

      usersTopic.pipeInput(
        "12345678",
        createUsersEvent("12345678", "MALE")
      )

      var pageId   = 0
      var viewTime = 1L
      for (_ <- 0 to 12) {
        pageId = pageId + 1
        viewTime = viewTime + 1L
        pageViewsTopic.pipeInput(
          "12345679",
          pageviews
            .newBuilder()
            .setPageid(s"page$pageId")
            .setUserid("12345678")
            .setViewtime(viewTime)
            .build(),
          setEventTime(1)
        )
      }

      assert(topPagesTopic.getQueueSize == 10)
    }
  }

  /**
    * Register topic schema in dummy schema registry
    *
    */
  private def registerTopicSchemaInDummySchemaRegistry(): Unit = {
    usersSerializer.configure(Map("schema.registry.url"     -> schemaRegistryURL).asJava, false)
    pageViewsSerializer.configure(Map("schema.registry.url" -> schemaRegistryURL).asJava, false)

    topPagesDeserializer.configure(Map("schema.registry.url" -> schemaRegistryURL).asJava, false)

    schemaRegistryClient.register("test.users-value", users.SCHEMA$)
    schemaRegistryClient.register("test.pageviews-value", pageviews.SCHEMA$)

    schemaRegistryClient.register("test.toppages-value", toppages.SCHEMA$)
  }
}
