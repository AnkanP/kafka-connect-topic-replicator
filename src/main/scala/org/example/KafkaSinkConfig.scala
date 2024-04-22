package org.example

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import java.util

// The below syntax for extending a java class by overloading constructor
/*
public KafkaSinkConfig(Map<?, ?> props) {
super(baseConfigDef(), props);
}
*/


class KafkaSinkConfig( val props: util.Map[_, _]) extends AbstractConfig(KafkaSinkConfig.baseConfigDef,props){

}

object KafkaSinkConfig{


  final val DESTINATION_TOPIC = "destination.topic"
  private final val DESTINATION_TOPIC_DOC = "target topic"
  final val DESTINATION_BOOTSTRAP_SERVERS = "destination.bootstrap.servers"
  private final val DESTINATION_BOOTSTRAP_SERVERS_DOC = "destination bootstrap servers"
  final val DESTINATION_SCHEMA_REGISTRY_URL = "destination.schema.registry.url"
  private final val DESTINATION_SCHEMA_REGISTRY_URL_DOC = "destination.schema.registry.url"

  final val PRODUCER_CONFIG = "kafka.producer.configs"
  final val PRODUCER_AVRO_CONFIG = "kafka.producer.avro.configs"

  def baseConfigDef: ConfigDef = {
    val config = new ConfigDef
    config.define(DESTINATION_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DESTINATION_TOPIC_DOC)
      .define(DESTINATION_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DESTINATION_BOOTSTRAP_SERVERS_DOC)
      .define(DESTINATION_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DESTINATION_SCHEMA_REGISTRY_URL_DOC)
      .define(PRODUCER_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "kafka producer configs")
      .define(PRODUCER_AVRO_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "kafka producer avro configs")
      //.define("enhanced.avro.schema.support", ConfigDef.Type.STRING, "false",ConfigDef.Importance.LOW, "Avro data configs")
      //.define("schemas.cache.config", ConfigDef.Type.STRING, "1000",ConfigDef.Importance.LOW, "Avro data configs")
      //.define("connect.meta.data", ConfigDef.Type.STRING,"true", ConfigDef.Importance.LOW, "Avro data configs")

  }

  val CONFIG_DEF: ConfigDef = baseConfigDef




}



