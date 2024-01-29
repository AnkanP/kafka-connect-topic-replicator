package org.example

import io.confluent.connect.avro.{AvroData, AvroDataConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.connect.errors.{ConnectException, RetriableException}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask, SinkTaskContext}

import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.Future
import scala.collection.JavaConversions._



@throws(classOf[ConnectException])
 class KafkaSinkTask extends SinkTask{

  private val log: Logger = LoggerFactory.getLogger(classOf[KafkaSinkTask])
  private val taskProperties: Properties = new Properties()
  //private var kafkaProducer: KafkaProducer[Object, Object] = null


  /** call back implementation */
  @throws(classOf[ConnectException])
  private val callback: Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Option(exception) match {
        case Some(e) => {
          println("Failed to produce:" + e.printStackTrace() )
          throw new ConnectException(e.getMessage)
          //throw exception
        }
        case None => println(s"Produced record at @topic: " + metadata.topic() + ", partition=" + metadata.partition() + ", offset=" +metadata.offset().toString)
      }
    }
  }


  override def start(map: util.Map[String, String]): Unit = {
    log.info("INSIDE START!!!!")
    map.foreach {case (key,value) => this.taskProperties.setProperty(key,value)}
    log.debug("PROPERTIES:" + taskProperties)

    val producerConfig = map.get(KafkaSinkConfig.PRODUCER_CONFIG)
    val bootstrapServers = map.get(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    val schemaRegistryUrl = map.get(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)

    try{
      if (KafkaSinkConnector.getKafkaProducerInstance == null) {
        println("Registering New Producer INSTANCE ON REBALANCE!!!")
        KafkaSinkConnector.registerKafkaProducerNew()
      }
    } catch {
      case e: Exception => {
        log.error(e.getMessage)
        throw new ConnectException(e.getMessage)
      }
      //throw new ConnectException(e.getMessage)
    } //finally kafkaProducer = null


  }

  @throws(classOf[ConnectException])
  //@throws(classOf[RetriableException])
  override def put(collection: util.Collection[SinkRecord]): Unit = {
    log.info("INSIDE PUT!!!!")
    for (record <- collection) {

      //connect schema
      val valSchema = record.valueSchema()
      val keySchema = record.keySchema()

      val avroData = new AvroData(new AvroDataConfig(taskProperties))

      //convert connect schema to avro schema
      val avroValSchema = avroData.fromConnectSchema(valSchema)
      val avroKeySchema = avroData.fromConnectSchema(keySchema)

      //convert to connect record to avro object
      val valObj: Object = avroData.fromConnectData(valSchema,record.value())
      val keyObj: Object = avroData.fromConnectData(keySchema,record.key())

      //4. Send producer record
      val  topic = taskProperties.getProperty(KafkaSinkConfig.DESTINATION_TOPIC)
      val producerRecord = new ProducerRecord[Object, Object](topic, null,record.timestamp(),keyObj, valObj)

      // Add headers
      for(header <- record.headers()){
        //producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))
        producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))

      }

      /** the kafka producer send can be synchronous or asynchronous
       * with asynchronous and a call back, we must implement the producer.flush in the flush method.
       * otherwise there is no guarantee records which are buffered are send to downstream */

      /** Non blocking producer */
      //kafkaProducer.send(producerRecord,callback)
      KafkaSinkConnector.getKafkaProducerInstance.send(producerRecord,callback)
     // kafkaProducer.send(producerRecord, new Callback() {
     //   def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
     //     if (e != null) e.printStackTrace()
     //     else log.debug("The offset of the record we just sent is: " + metadata.offset)
     //   }
     // })

      /** Blocking producer */
     // try {
     //   kafkaProducer.send(producerRecord).get()
     // } catch {
     //   case e: Exception =>
     //     log.error(e.getMessage())
     //     e.printStackTrace()
     //     //throw new RetriableException(e.printStackTrace())
     //     throw new ConnectException(e.getMessage)
     //    }

      }
  }



  @throws(classOf[ConnectException])
  override def stop(): Unit = {
    log.info("INSIDE STOP!!!!")
    log.info("Shutting down kafka producer!!")
    try {
      KafkaSinkConnector.deregisterKafkaProducerInstance
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new ConnectException(e.getMessage)
    } //finally KafkaSinkConnector.deregisterKafkaProducerInstance
  }


  override def initialize(context: SinkTaskContext): Unit = {
    log.info("INSIDE INITIALIZE!!!!")
    for (x <- context.assignment()) {
      log.debug("PARTITIONS ASSIGNED: " + x.partition())
    }
    super.initialize(context)

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
  @throws(classOf[ConnectException])
  override def close(partitions: util.Collection[TopicPartition]): Unit = {
    log.info("INSIDE CLOSE!!!!")
    try {
      KafkaSinkConnector.deregisterKafkaProducerInstance
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new ConnectException(e.getMessage)
    }
    super.close(partitions)
  }

  override def version(): String = {
    new KafkaSinkConnector().version()
  }


  @throws(classOf[ConnectException])
  override def flush(currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    log.info("INSIDE FLUSH!!!!")
    for (offset <- currentOffsets){
      log.debug("Flushing the consumer offsets {} " + offset._1.topic() + ": partition: " + offset._1.partition().toString + " :offset: " + offset._2.offset().toString + "-" + offset._2.metadata().toString)
    }

    /** kafkaProducer.flush() call for asynchronous non blocking producer send */
    /** No need to implement incase of blocking producer send */
    try {
      KafkaSinkConnector.getKafkaProducerInstance.flush()
      } catch {
        case e: Exception =>
        throw new ConnectException(e.getMessage)
        }
  }


}

