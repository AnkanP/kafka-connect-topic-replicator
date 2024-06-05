package org.example

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.{ConnectException, RetriableException}
import org.apache.kafka.connect.sink.{SinkConnector, SinkConnectorContext}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConversions.mapAsJavaMap

object KafkaSinkConnectorBlocking{

  var counter = 0
  lazy val props: Properties = new Properties
  var kafkaProducer: KafkaProducer[Object, Object] = null
  private final val logger1: Logger = LoggerFactory.getLogger(classOf[KafkaSinkConnectorBlocking])


  private final def registerProducerProperties(producerConfig: String, bootstrapServers: String, schemaRegistryUrl: String) = {
    val hashMap = producerConfig.split(',').map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
    val javaMap: util.Map[_, _] = mapAsJavaMap(hashMap)
    props.putAll(javaMap)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty("schema.registry.url", schemaRegistryUrl)

  }

  def registerKafkaProducerNew(): Unit = {
    counter = counter + 1
    println("CALLING FUNCTION CALL NEW: " + counter.toString)
    //deregisterKafkaProducerInstance()
    if(kafkaProducer == null) {
      kafkaProducer =  new KafkaProducer(props)
    }

  }

  def getKafkaProducerInstance: KafkaProducer[Object, Object] = {
    println("CALLING INSIDE PRODUCER CALL: " + counter.toString)

    try{
      this.kafkaProducer
    } catch {
      case e: Exception => {
        logger1.error(e.getMessage)
        throw new ConnectException(e.getMessage)
      }

    }

  }

  def deregisterKafkaProducerInstance(): Unit = {
    println("CALLING INSIDE PRODUCER CLOSE: " + counter.toString)

    try {
      this.kafkaProducer.close()
    } catch {
      case e: Exception =>{
        logger1.info("Unable to close producer!!. Perhaps it's closed. Do proper Exception catch")
        logger1.error(e.getMessage)
      }
      //throw new ConnectException(e.getMessage)
    } finally kafkaProducer = null


    //this.kafkaProducer = null
  }



}

class KafkaSinkConnectorBlocking extends SinkConnector{

  private final val logger: Logger = LoggerFactory.getLogger(classOf[KafkaSinkConnectorBlocking])
  private var connectorConfig: KafkaSinkConfig = null
  var configProps: util.Map[String, String] = null
  // var kafkaProducer: KafkaProducer[Object, Object] = null
  var producerConfig = ""
  var bootstrapServers = ""
  var schemaRegistryUrl = ""

  //final object x = 'a'

  ///override def context(): SinkConnectorContext = super.context()  //AP CHANGED


  override def start(props: util.Map[String, String]): Unit = {

    // When the connector starts, this method is called which provides a new instance of our custom configuration to the kafka connect framework
    this.connectorConfig =  new KafkaSinkConfig(props)

    this.configProps = Collections.unmodifiableMap(props)

    this.producerConfig = props.get(KafkaSinkConfig.PRODUCER_CONFIG)
    this.bootstrapServers = props.get(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    this.schemaRegistryUrl = props.get(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)
    //this.kafkaProducer =
    KafkaSinkConnectorBlocking.registerProducerProperties(producerConfig, bootstrapServers, schemaRegistryUrl)
    KafkaSinkConnectorBlocking.registerKafkaProducerNew()

    /** Initialize producer */
    //CustomKafkaProducer.registerKafkaProducerNew(producerConfig, bootstrapServers, schemaRegistryUrl)
    //KafkaSinkConnector.registerKafkaProducerNew(producerConfig, bootstrapServers, schemaRegistryUrl)

  }

  @throws(classOf[RetriableException])
  @throws(classOf[RuntimeException])
  @throws(classOf[ConnectException])
  override def taskClass(): Class[_ <: Task] = classOf[KafkaSinkTaskBlocking]

  //3  It just has to determine the number of input tasks, which may require contacting the remote service it is pulling data from, and then divvy them up
  //which returns a list of maps containing the configuration properties each task will use to stream data into or out of Kafka:
  //The method accepts an int value for the maximum number of tasks to run in parallel and is pulled from the tasks.max configuration property that is provided on startup.
  //https://www.confluent.io/blog/create-dynamic-kafka-connect-source-connectors/

  //On startup, the Kafka Connect framework will pass each configuration map contained in the list returned by taskConfigs to a task.


  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val configs = new util.ArrayList[util.Map[String, String]]

    println("INSIDE TASKS CONFIGS!!!")

    //if( KafkaSinkConnector.getKafkaProducerInstance == null) {
    //  println("Registering New Producer INSTANCE!!!")
    // KafkaSinkConnector.registerKafkaProducerNew(producerConfig, bootstrapServers, schemaRegistryUrl)
    // }
    //else
    //{
    //  logger.info("INSIDE TASK CONFIG ELSE")
    //  KafkaSinkConnector.deregisterKafkaProducerInstance
    //  KafkaSinkConnector.registerKafkaProducerNew(producerConfig, bootstrapServers, schemaRegistryUrl)
    //}

    for (i <- 0 until maxTasks) {

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
