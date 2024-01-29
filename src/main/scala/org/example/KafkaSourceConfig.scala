package org.example

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import java.util

class KafkaSourceConfig ( val props: util.Map[_, _]) extends AbstractConfig(KafkaSourceConfig.baseConfigDef,props){

}

object KafkaSourceConfig{


  final val SOURCE_TOPIC = "source.topic"
  final val SOURCE_TOPIC_DOC = "source topic"
  final val SOURCE_BOOTSTRAP_SERVERS = "SOURCE.bootstrap.servers"
  final val SOURCE_BOOTSTRAP_SERVERS_DOC = "SOURCE bootstrap servers"
  final val SOURCE_KEY_SERIALIZER = "SOURCE.key.serializer"
  final val SOURCE_KEY_SERIALIZER_DOC = "SOURCE.key.serializer"
  final val SOURCE_VALUE_SERIALIZER = "SOURCE.value.serializer"
  final val SOURCE_VALUE_SERIALIZER_DOC = "SOURCE.value.serializer"
  final val SOURCE_SCHEMA_REGISTRY_URL = "SOURCE.schema.registry.url"
  final val SOURCE_SCHEMA_REGISTRY_URL_DOC = "SOURCE.schema.registry.url"

  final val CONSUMER_CONFIG = "kafka.consumer.configs"

  def baseConfigDef: ConfigDef = {
    val config = new ConfigDef
    config.define(SOURCE_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_TOPIC_DOC)
      //.define(SOURCE_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_BOOTSTRAP_SERVERS_DOC)
      //.define(SOURCE_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_SCHEMA_REGISTRY_URL_DOC)
      //.define(CONSUMER_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "kafka producer configs")


    //.define(SOURCE_KEY_SERIALIZER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_KEY_SERIALIZER_DOC)
    // .define(SOURCE_VALUE_SERIALIZER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_VALUE_SERIALIZER_DOC)

  }

  val CONFIG_DEF: ConfigDef = baseConfigDef




}