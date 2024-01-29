package org.example

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.slf4j.LoggerFactory
import java.util.Collections
import java.util

class KafkaSourceConnector extends SourceConnector {


  final val logger = LoggerFactory.getLogger(classOf[KafkaSinkConnector])
  var connectorConfig: KafkaSourceConfig = null
  var configProps: util.Map[String, String] = null


  override def start(props: util.Map[String, String]): Unit = {// When the connector starts, this method is called which provides a new instance of our custom configuration to the kafka connect framework

    this.connectorConfig = new KafkaSourceConfig(props)
    this.configProps = Collections.unmodifiableMap(props)
  }



  override def taskClass(): Class[_ <: Task] = classOf[KafkaSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {

    val configs = new util.ArrayList[util.Map[String, String]]

    for (i <- 0 until maxTasks) {

      // Only one input partition makes sense.
      val config = new util.HashMap[String, String]()
      config.putAll(this.configProps)
      configs.add(config)

    }

    configs
  }

  override def stop(): Unit = {
    //
  }

  override def config(): ConfigDef = {
    KafkaSourceConfig.CONFIG_DEF
  }

  override def version(): String = {
    Version.getVersion

  }
}
