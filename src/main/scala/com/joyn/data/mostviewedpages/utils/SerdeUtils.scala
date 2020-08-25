package com.joyn.data.mostviewedpages.utils

import java.util
import java.util.Collections

import com.joyn.data.mostviewedpages.configuration.Configuration._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord

object SerdeUtils {

  var schemaRegistry: String = confluent.local.schemaRegistry

  if (environment.equalsIgnoreCase("dev")) {
    schemaRegistry = confluent.dev.schemaRegistry
  }

  private val serdeConfig: util.Map[String, String] = Collections.singletonMap(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
    schemaRegistry)

  def configValueSerde[A <: SpecificRecord]: SpecificAvroSerde[A] = {
    val valueSerde: SpecificAvroSerde[A] = new SpecificAvroSerde
    valueSerde.configure(serdeConfig, false)
    valueSerde
  }

}
