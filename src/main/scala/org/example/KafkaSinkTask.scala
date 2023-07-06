package org.example

import io.confluent.connect.avro.{AvroData, AvroDataConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask, SinkTaskContext}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConversions._


class KafkaSinkTask extends SinkTask{

  private val log = LoggerFactory.getLogger(classOf[KafkaSinkTask])

  var topic: String = ""
  var producerConfig: String = ""
  var bootstrapServers: String = ""
  var schemaRegistryUrl: String = ""
  var taskProperties: Properties = new Properties()

  /* lazy objects get evaluated at variable call */

    lazy val properties: Properties = buildProperties
    lazy val kafkaProducer: KafkaProducer[Object, Object] = new KafkaProducer(this.properties)


  //3. Send the record to kafka
 lazy val callback: Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Option(exception) match {
        case Some(e) => {
          println(s"Failed to produce: ${e.printStackTrace()}")
          throw new ConnectException(e.getMessage)

        }
        case None => println(s"Produced record at $metadata")
      }
    }
  }


  override def start(map: util.Map[String, String]): Unit = {
    log.info("INSIDE START: {}")
    map.foreach {case (key,value) => this.taskProperties.setProperty(key,value)}

    topic = map.get(KafkaSinkConfig.DESTINATION_TOPIC)
    producerConfig = map.get(KafkaSinkConfig.PRODUCER_CONFIG)
    bootstrapServers = map.get(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    schemaRegistryUrl = map.get(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)

  }

  override def put(collection: util.Collection[SinkRecord]): Unit = {
    log.info("INSIDE PUT: {}")
    for (record <- collection) {
      log.info("record : {}",  record.value)


      //connect schema
      val valSchema = record.valueSchema()
      val keySchema = record.keySchema()

      log.info("PROPERTIES:" + taskProperties)

     val avroData = new AvroData(new AvroDataConfig(taskProperties))

      //convert connect schema to avro schema
      //val avroValSchema = avroData.fromConnectSchema(valSchema)
      //val avroKeySchema = avroData.fromConnectSchema(keySchema)

      //convert to connect record to avro object
      val valObj: Object = avroData.fromConnectData(valSchema,record.value())
      val keyObj: Object = avroData.fromConnectData(keySchema,record.key())






      //4. Send producer record


      val producerRecord = new ProducerRecord[Object, Object](topic, null,record.timestamp(),keyObj, valObj)

    //  val header: Iterable[Header] = null


      // Add headers
      for(header <- record.headers()){
        producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))

      }


      kafkaProducer.send(producerRecord,callback)
     // try this.kafkaProducer.send(producerRecord,callback)
     // catch {
     //   case e: Exception =>
     //       log.error(e.getMessage())
     //       e.printStackTrace()
     //       throw new ConnectException(e.getMessage)
     //     }
     // finally this.stop()
      }
  }

  override def stop(): Unit = {
    log.info("Shutting down kafka producer!!")
    try {
    if(kafkaProducer !=null)  kafkaProducer.close(Duration.ofSeconds(30))
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new ConnectException(e.getMessage)
    }
  }


  override def initialize(context: SinkTaskContext): Unit = {
    log.info("INSIDE INITIALIZE: {}")
    super.initialize(context)
  }

  override def open(partitions: util.Collection[TopicPartition]): Unit = {
    log.info("INSIDE OPEN: {}")
    super.open(partitions)
  }

  override def close(partitions: util.Collection[TopicPartition]): Unit = {
    log.info("INSIDE CLOSE: {}")
    try {
      if (kafkaProducer != null) kafkaProducer.close(Duration.ofSeconds(30))
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        throw new ConnectException(e.getMessage)
    }
    super.close(partitions)
  }

  override def version(): String = {
    log.info("INSIDE VERSION: {}")
    new KafkaSinkConnector().version()
  }

  override def flush(currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    log.info("Flushing kafka producer for {}")
    kafkaProducer.flush()
  }

  def buildProperties: Properties = {
    log.info("INSIDE BUILD PROPERTIES: {}")
    val props: Properties = new Properties

    val hashMap = producerConfig.split(',').map(_.split("=")).map {case Array(k,v) => (k,v) }.toMap

    val javaMap: util.Map[_,_] = mapAsJavaMap(hashMap)

    props.putAll(javaMap)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty("schema.registry.url", schemaRegistryUrl)

    props
  }


}

