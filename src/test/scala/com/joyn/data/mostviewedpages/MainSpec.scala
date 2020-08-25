package com.joyn.data.mostviewedpages

import java.time.{Duration, Instant}

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import ksql._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.StreamsBuilder

trait MainSpec {

  // Input Topics
  protected val testUsersTopicName     = "test.users"
  protected val testPageViewsTopicName = "test.pageviews"

  // Output Topics
  protected val testTopPagesTopicName = "test.toppages"

  protected val testApplicationId = "test_app_id"

  protected val brokersUrl = "dummy:1234"
  protected val schemaRegistryClient: SchemaRegistryClient =
    MockSchemaRegistry.getClientForScope("dummy")
  protected val schemaRegistryURL = "mock://dummy"

  // Serializers
  val usersSerializer: Serializer[users] =
    new SpecificAvroSerde[users](schemaRegistryClient).serializer()
  val pageViewsSerializer: Serializer[pageviews] =
    new SpecificAvroSerde[pageviews](schemaRegistryClient).serializer()

  // Deserializers
  val pageViewsDeserializer: Deserializer[pageviews] =
    new SpecificAvroSerde[pageviews](schemaRegistryClient).deserializer()
  val topPagesDeserializer: Deserializer[toppages] =
    new SpecificAvroSerde[toppages](schemaRegistryClient).deserializer()

  protected def buildTestTopology: StreamsBuilder => Topology =
    (builder: StreamsBuilder) => builder.build()

  protected def initTestTopology: StreamsBuilder = new StreamsBuilder()

  /**
    * Build Test topology properties
    * @return Topology properties
    */
  protected def getTestTopologyProperties: java.util.Properties = {
    val testTopologyProperties = new java.util.Properties()

    testTopologyProperties.put(
      StreamsConfig.APPLICATION_ID_CONFIG,
      testApplicationId
    )
    testTopologyProperties.put(
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
      brokersUrl
    )
    testTopologyProperties.put(
      StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      Serdes.String().getClass.getName
    )
    testTopologyProperties.put(
      StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde"
    )
    testTopologyProperties.put(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      schemaRegistryURL
    )
    testTopologyProperties
  }

  /**
    * Create Input topic in the topology
    * @param topology Test Topology.
    * @param topicName Input topic name.
    * @param serializer Avro value serializer.
    * @return
    */
  protected def createInputTopic[T](
      topology: TopologyTestDriver,
      topicName: String,
      serializer: Serializer[T]
  ): TestInputTopic[String, T] = {
    topology.createInputTopic(topicName,
                              new StringSerializer,
                              serializer,
                              Instant.ofEpochMilli(0L),
                              Duration.ZERO)
  }

  /**
    * Create output topic in the topilogy
    * @param topology Test Topology.
    * @param topicName Output topic name.
    * @param deserializer Avro value Deserializer.
    * @return
    */
  protected def createOutputTopic[T](
      topology: TopologyTestDriver,
      topicName: String,
      deserializer: Deserializer[T]
  ): TestOutputTopic[String, T] =
    topology.createOutputTopic(topicName, new StringDeserializer, deserializer)

}
