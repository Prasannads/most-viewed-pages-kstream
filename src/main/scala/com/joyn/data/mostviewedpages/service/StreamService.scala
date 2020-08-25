package com.joyn.data.mostviewedpages.service

import java.time.Duration
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.{KafkaStreams, Topology}

class StreamService(topology: Topology, kStreamProperties: Properties) extends LazyLogging {

  /**
    * Start Kstream
    */
  @throws(classOf[Exception])
  def start(): Unit = {
    logger.info("Starting the kstream application...")
    val kafkaStreams: KafkaStreams = new KafkaStreams(topology, kStreamProperties)
    kafkaStreams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      kafkaStreams.close(Duration.ofSeconds(10))
    }
  }
}
