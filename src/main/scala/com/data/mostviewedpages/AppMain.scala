package com.data.mostviewedpages

import com.data.mostviewedpages.configuration.Configuration
import com.data.mostviewedpages.service.StreamService
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}

trait AppMain extends LazyLogging {

  // Instantiate the streams builder.
  protected def initTopology: StreamsBuilder = new StreamsBuilder()

  /**
    * Build Topology
    * @return Topology
    */
  protected def buildTopology: StreamsBuilder => Topology =
    (builder: StreamsBuilder) => builder.build()

  /**
    * Generic Method to build a Global KTable.
    * @param builder Streams builder.
    * @param topicName Name of the topic on which GlobalKtable will be built upon.
    * @param KeySerde Serde for the Key.
    * @param valueSerde Serde for the value.
    * @return
    */
  def buildGlobalKTable[K, V](builder: StreamsBuilder, topicName: String)(
      implicit KeySerde: Serde[K],
      valueSerde: Serde[V]): GlobalKTable[K, V] = {
    val materializedView: Materialized[K, V, ByteArrayKeyValueStore] =
      Materialized
        .as(topicName)
    materializedView.withCachingEnabled()

    builder.globalTable[K, V](topicName, materializedView)
  }

  /**
    * Start Kstream server
    * @param topology Topology built with transformation logic.
    * @param applicationId ApplicationId from the config.
    */
  protected def stream(topology: Topology, applicationId: String): Unit = {
    val kstreamProperties =
      Configuration.confluent.getKafkaConfiguration(Configuration.environment, applicationId)

    val kstreamService = new StreamService(topology, kstreamProperties)
    try {
      kstreamService.start()
    } catch {
      case e: Exception =>
        logger.error("KStream crash", e)
    }
  }

}
