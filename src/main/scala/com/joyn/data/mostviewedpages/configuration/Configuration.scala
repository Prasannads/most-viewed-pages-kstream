package com.joyn.data.mostviewedpages.configuration

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import pureconfig._
import pureconfig.generic.auto._

case class ConfluentBrokerConfig(brokers: String, schemaRegistry: String)
case class ConfluentConfig(local: ConfluentBrokerConfig,
                           dev: ConfluentBrokerConfig,
                           autoRegisterSchema: String,
                           sslEndpointIdentificationAlgorithm: String,
                           saslMechanism: String,
                           securityProtocol: String,
                           saslJaasConfig: String,
                           producerACKS: Int,
                           clientId: String) {

  def getKafkaConfiguration(environment: String, applicationId: String): Properties = {

    var broker: String         = local.brokers
    var schemaRegistry: String = local.schemaRegistry

    if (environment.equalsIgnoreCase("dev")) {
      broker = dev.brokers
      schemaRegistry = dev.schemaRegistry
    }

    val avroSerializer   = "io.confluent.kafka.serializers.KafkaAvroSerializer"
    val stringSerializer = "org.apache.kafka.common.serialization.StringSerializer"
    val properties       = new Properties()

    // producer properties config
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, avroSerializer)
    properties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchema)
    properties.put(ProducerConfig.ACKS_CONFIG, producerACKS.toString)

    // confluent config
    properties.put("confluent.monitoring.interceptor.sasl.mechanism", saslMechanism)
    properties.put("confluent.monitoring.interceptor.security.protocol", securityProtocol)
    properties.put("confluent.monitoring.interceptor.sasl.jaas.config", saslJaasConfig)
    properties.put("confluent.monitoring.interceptor.client.id", "interceptor-" + clientId)
    properties.put("ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm)
    properties.put("sasl.mechanism", saslMechanism)
    /*properties.put("request.timeout.ms", requestTimeoutMs.toString)
    properties.put("retry.backoff.ms", retryBackoffMs.toString)*/
    properties.put("sasl.jaas.config", saslJaasConfig)
    properties.put("security.protocol", securityProtocol)
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)

    // stream config
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                   "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde")

    // for local test
    properties.put(StreamsConfig.STATE_DIR_CONFIG, "tmp")

    properties
  }
}

case class AppConfig(pageViewsAppId: String,
                     usersTopic: String,
                     pageViewTopic: String,
                     topPagesTopic: String)

case class Config(confluent: ConfluentConfig, app: AppConfig)

/**
  * Load configuration from remote configuration management
  */
object Configuration extends LazyLogging {
  val environment: String = sys.env.getOrElse("KSTREAM_ENVIRONMENT", "dev")
  logger.info(s"Loading the configuration for application for environment $environment")
  val configuration: Config = ConfigSource.default.load[Config] match {
    case Right(c) =>
      logger.info(s"Loaded the configuration: \n$c")
      c
    case Left(c) =>
      logger.error(s"Failed to load the configuration: ${c.toString}")
      throw new IllegalArgumentException("bad configuration")
  }

  val confluent: ConfluentConfig = configuration.confluent
  val app: AppConfig             = configuration.app
  logger.info(confluent.toString)
  logger.info(app.toString)

}
