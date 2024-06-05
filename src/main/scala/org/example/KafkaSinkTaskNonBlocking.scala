package org.example

import io.confluent.connect.avro.{AvroData, AvroDataConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.connect.errors.{ConnectException, RetriableException}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask, SinkTaskContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.mapAsJavaMapConverter



@throws(classOf[RetriableException])
@throws(classOf[RuntimeException])
@throws(classOf[ConnectException])
class KafkaSinkTaskNonBlocking extends SinkTask{

  private val log: Logger = LoggerFactory.getLogger(classOf[KafkaSinkTaskNonBlocking])

  val taskProperties: Properties = new Properties()
  /** BATCH RETRIES */
   private var offsetAndMetdadataMap = scala.collection.mutable.Map.empty[TopicPartition, OffsetAndMetadata]

  private object initializationObject {
    lazy val TOPIC: String = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_TOPIC)
    private lazy val avroDataConfigs: String = taskProperties.getProperty(KafkaSinkConfig.PRODUCER_AVRO_CONFIG)
    private lazy val configMap: Map[String,String] = avroDataConfigs.split(',').map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
    private lazy val avroDataConfigProps: Properties = new Properties
    configMap.foreach {case (key,value) => avroDataConfigProps.setProperty(key,value)}
    private lazy val avroDataConfig: AvroDataConfig = new AvroDataConfig(avroDataConfigProps)
    lazy val avroData: AvroData = new AvroData(avroDataConfig)

    var remainingRetries: Int = taskProperties.getProperty(KafkaSinkConfig.MAX_RETRIES).toInt
    val retryBackoffMs: Long = taskProperties.getProperty(KafkaSinkConfig.MAX_RETRIES_BACKOFF_MS).toLong

  }


  //private var kafkaProducer: KafkaProducer[Object, Object] = null

  /** CHANGES */
  //private var avroDataConfig: AvroDataConfig = _
  //private var avroData: AvroData = _
  //private var TOPIC: String = _

  /** call back implementation */
  //  @throws(classOf[ConnectException])
  //  private val callback: Callback = new Callback {
  //    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
  //      Option(exception) match {
  //        case Some(e) => {
  //          println("Failed to produce:" + e.printStackTrace() )
  //          throw new ConnectException(e.getMessage)
  //          //throw exception
  //        }
  //        case None => println(s"Produced record at @topic: " + metadata.topic() + ", partition=" + metadata.partition() + ", offset=" +metadata.offset().toString)
  //      }
  //    }
  //  }


  override def start(map: util.Map[String, String]): Unit = {
    log.info("INSIDE START!!!!")
    //map.foreach {case (key,value) => taskProperties.setProperty(key,value)}
    map.foreach {case (key,value) => taskProperties.setProperty(key,value)}
    log.debug("PROPERTIES:" + taskProperties)

    //val producerConfig = map.get(KafkaSinkConfig.PRODUCER_CONFIG)
    //val bootstrapServers = map.get(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    //val schemaRegistryUrl = map.get(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)

    initializationObject

    /* INITIALIZATION */
    //TOPIC = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_TOPIC)
    //val avroDataConfigMap = map.get(KafkaSinkConfig.PRODUCER_AVRO_CONFIG)
    //val avroDataConfigProps: Properties = new Properties

    //val hashMap = avroDataConfigMap.split(',').map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
    //val javaMap: util.Map[_, _] = mapAsJavaMap(hashMap)
    //avroDataConfigProps.putAll(javaMap)
    //avroDataConfig = new AvroDataConfig(avroDataConfigProps)
    //avroData = new AvroData(avroDataConfig)

    try{
      if (KafkaSinkConnectorNonBlocking.getKafkaProducerInstance == null) {
        println("Registering New Producer INSTANCE ON REBALANCE!!!")
        KafkaSinkConnectorNonBlocking.registerKafkaProducerNew()
      }
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new RetriableException(e.getMessage)
      //throw new ConnectException(e.getMessage)
    } //finally kafkaProducer = null


  }

  @throws(classOf[RetriableException])
  @throws(classOf[ConnectException])
  override def put(collection: util.Collection[SinkRecord]): Unit = {
    log.info("INSIDE PUT!!!!")
    for (record <- collection) {

      //connect schema
      val valSchema = record.valueSchema()
      val keySchema = record.keySchema()

      //val avroData = new AvroData(new AvroDataConfig(taskProperties))

      //convert connect schema to avro schema
      //val avroValSchema = initializationObject.avroData.fromConnectSchema(valSchema)
      //val avroKeySchema = initializationObject.avroData.fromConnectSchema(keySchema)

      //print("VALUE SCHEMA: " + avroValSchema.toString(true))
      //print("KEY SCHEMA: " + avroKeySchema.toString(true))

      //convert to connect record to avro object
      val valObj: Object = initializationObject.avroData.fromConnectData(valSchema,record.value())
      val keyObj: Object = initializationObject.avroData.fromConnectData(keySchema,record.key())

      //print("VALUE : " + valObj.toString)
      //print("KEY : " + keyObj.toString)

      //4. Send producer record
      // val  topic = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_TOPIC)
      val producerRecord = new ProducerRecord[Object, Object](initializationObject.TOPIC, null,record.timestamp(),keyObj, valObj)

      // Add headers
      for(header <- record.headers()){
        //producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))
        producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))

      }

      /** the kafka producer send can be synchronous or asynchronous
       * with asynchronous and a call back, we must implement the producer.flush in the flush method.
       * otherwise there is no guarantee records which are buffered are send to downstream */

      /** Non blocking producer */
      log.info("BATCH RETRY COUNT: " + initializationObject.remainingRetries.toString)
      KafkaSinkConnectorNonBlocking.getKafkaProducerInstance.send(producerRecord,
        (metadata: RecordMetadata, exception: Exception) => {
          Option(exception) match {
            case Some(e) => {
              initializationObject.remainingRetries = initializationObject.remainingRetries - 1
              log.info("TIMEOUT:-" + initializationObject.retryBackoffMs.toString)
              context.timeout(initializationObject.retryBackoffMs)

              //Fail for any exception
              // The exceptions dont work becaue the callback code is running in a background I/O.
              throw new ConnectException(e.getMessage)
              stop()
              // For this to fail the task, make sure to have a small no of retry duration and increase the offset interval to atleast 5mins


             //if (initializationObject.remainingRetries > 0)
             //  throw new RetriableException(e.getMessage)
             //else
             //  throw new ConnectException(e.getMessage)
             ////throw exception
            }
            case None => {
              log.info(s"Produced record at @topic: " + metadata.topic() + ", partition=" + metadata.partition() + ", offset=" + metadata.offset().toString)
              // populate with consumer values
              val offsetAndMetadata: OffsetAndMetadata = new OffsetAndMetadata(record.kafkaOffset() + 1L, "")
              val partition: TopicPartition = new TopicPartition(metadata.topic(), record.kafkaPartition())

              offsetAndMetdadataMap.put(partition, offsetAndMetadata)
            }
          }
          print("")
      })
    }
  }



  //@throws(classOf[RuntimeException])
  override def stop(): Unit = {
    log.info("INSIDE STOP!!!!")
    log.info("Shutting down kafka producer!!")
    try {
      KafkaSinkConnectorNonBlocking.deregisterKafkaProducerInstance()
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
      //throw new RuntimeException(e.getMessage)
    } //finally KafkaSinkConnector.deregisterKafkaProducerInstance
  }


  override def initialize(context: SinkTaskContext): Unit = {
    log.info("INSIDE INITIALIZE!!!!")
    for (x <- context.assignment()) {
      log.debug("PARTITIONS ASSIGNED: " + x.partition())
    }
    //context.timeout(30)
    super.initialize(context) //disabled for testing

  }

  /** Note that any errors raised from close() or open() will cause the task to stop, report a failure status, and the corresponding consumer instance to close.
   * This consumer shutdown triggers a rebalance, and topic partitions for this task will be reassigned to other tasks of this connector. */

  /** The open() method is used to create writers for newly assigned partitions in case of consumer rebalance.
   * This method will be called after partition re-assignment completes and before the SinkTask starts fetching data */
  override def open(partitions: util.Collection[TopicPartition]): Unit = {
    log.info("INSIDE OPEN!!!!")
    //val producerConfig = taskProperties.getProperty(KafkaSinkConfig.PRODUCER_CONFIG)
    //val bootstrapServers = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    //val schemaRegistryUrl = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)
    //kafkaProducer = CustomKafkaProducer.registerKafkaProducerNew(producerConfig,bootstrapServers,schemaRegistryUrl)

    //kafkaProducer = CustomKafkaProducer.getKafkaProducerInstance
    //kafkaProducer = KafkaSinkConnector.getKafkaProducerInstance

    log.debug("PARTITIONS ASSIGNED TO TASK" + partitions.toString)
    for(partition <- partitions){
      log.debug("PARTITIONS OPENED: " + partition.partition())

    }

    super.open(partitions) // deprecated
  }

  /**
   * The close() method is used to close writers for partitions assigned to the SinkTask
   * This method will be called before a consumer rebalance operation starts and after the SinkTask stops fetching data. After being closed, Connect will not write any records to the task until a new set of partitions has been opened.
   * */
  //@throws(classOf[RuntimeException])
  override def close(partitions: util.Collection[TopicPartition]): Unit = {
    log.info("INSIDE CLOSE!!!!")
    try {
      KafkaSinkConnectorNonBlocking.deregisterKafkaProducerInstance()
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
      //throw new RuntimeException(e.getMessage)
    }
    super.close(partitions)
  }

  override def version(): String = {
    new KafkaSinkConnectorNonBlocking().version()
  }



// override def flush(currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
//   log.info("INSIDE FLUSH!!!!")
//   for (offset <- currentOffsets){
//     log.debug("Flushing the consumer offsets {} " + offset._1.topic() + ": partition: " + offset._1.partition().toString + " :offset: " + offset._2.offset().toString + "-" + offset._2.metadata())
//   }

//   /** kafkaProducer.flush() call for asynchronous non blocking producer send */
//   /** No need to implement incase of blocking producer send */
//
// }


  @throws(classOf[ConnectException])
  @throws(classOf[RuntimeException])
  override def preCommit(currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]): util.Map[TopicPartition, OffsetAndMetadata] = {

    log.info("INSIDE PRE-COMMIT!")
    val manualOffsets: util.Map[TopicPartition, OffsetAndMetadata] = offsetAndMetdadataMap.asJava
    log.info("AUTO COMMIT OFFSETS:- " + currentOffsets.toString)
    log.info("MANUAL COMMIT OFFSETS:- "  + manualOffsets.toString)

    try {
      KafkaSinkConnectorNonBlocking.getKafkaProducerInstance.flush()
    } catch {
      case e: Exception =>
        log.info("FLUSH ERROR RAISED!!")
        throw new RuntimeException(e.getMessage)
    }

    super.preCommit(currentOffsets)
    currentOffsets
  }


}

