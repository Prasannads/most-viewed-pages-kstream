#!/bin/bash
#create Schemas
for schema in $(ls /usr/bin/schemas|grep '.json'|sed 's/\(.*\).json/\1/g'); do
  curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" -d @/usr/bin/schemas/${schema}.json http://schema-registry:8081/subjects/${schema}-value/versions
done
#Create topics
wget https://mirrors.ae-online.de/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz
mv kafka_2.13-2.6.0.tgz kafka_2.13-2.6.0.tar.gz
gunzip kafka_2.13-2.6.0.tar.gz
tar -xf  kafka_2.13-2.6.0.tar
cd kafka_2.13-2.6.0/
for topic in $(ls /usr/bin/schemas|grep '.json'|sed 's/\(.*\).json/\1/g'); do
  create_topic="bin/kafka-topics.sh --create --bootstrap-server broker:29092 --replication-factor 1 --partitions 3 --topic ${topic}"
  eval "$create_topic"
done
#Create Connectors
curl -X POST -H "Content-Type: application/json" -d @/usr/bin/connector-configuration/connector_datagen-pageviews_config.json http://connect:8083/connectors
curl -X POST -H "Content-Type: application/json" -d @/usr/bin/connector-configuration/connector_datagen-users_config.json http://connect:8083/connectors