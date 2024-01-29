package org.example

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util
import java.util.{Collections, Properties}


class CustomKafkaSinkConnector  extends SinkConnector {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[CustomKafkaSinkConnector])
  private var connectorConfig: KafkaSinkConfig = null
  private var configProps: util.Map[String, String] = null

  override def start(props: util.Map[String, String]): Unit = {

    // When the connector starts, this method is called which provides a new instance of our custom configuration to the kafka connect framework
    this.connectorConfig = new KafkaSinkConfig(props)

    this.configProps = Collections.unmodifiableMap(props)

  }

  override def taskClass(): Class[_ <: Task] = classOf[CustomKafkaSinkTask]

  //3  It just has to determine the number of input tasks, which may require contacting the remote service it is pulling data from, and then divvy them up
  //which returns a list of maps containing the configuration properties each task will use to stream data into or out of Kafka:
  //The method accepts an int value for the maximum number of tasks to run in parallel and is pulled from the tasks.max configuration property that is provided on startup.
  //https://www.confluent.io/blog/create-dynamic-kafka-connect-source-connectors/

  //On startup, the Kafka Connect framework will pass each configuration map contained in the list returned by taskConfigs to a task.


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
    // Nothing to do since FileStreamSinkConnector has no background monitoring.
  }

  // The below method ensures the validation happens while submitting the connect job.
  override def config(): ConfigDef = {
    KafkaSinkConfig.CONFIG_DEF
  }

  override def version(): String = {
    //AppInfoParser.getVersion
    Version.getVersion

  }


}
