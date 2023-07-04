package org.example

import io.confluent.connect.avro.{AvroData, AvroDataConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.LoggerFactory

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
        case Some(e) => println(s"Failed to produce: ${e.printStackTrace()}")
        case None => println(s"Produced record at $metadata")
      }
    }
  }


  override def start(map: util.Map[String, String]): Unit = {

    map.foreach {case (key,value) => this.taskProperties.setProperty(key,value)}

    this.topic = map.get(KafkaSinkConfig.DESTINATION_TOPIC)
    this.producerConfig = map.get(KafkaSinkConfig.PRODUCER_CONFIG)
    this.bootstrapServers = map.get(KafkaSinkConfig.DESTINATION_BOOTSTRAP_SERVERS)
    this.schemaRegistryUrl = map.get(KafkaSinkConfig.DESTINATION_SCHEMA_REGISTRY_URL)

  }

  override def put(collection: util.Collection[SinkRecord]): Unit = {

    for (record <- collection) {
      log.info("record : {}",  record.value)


      //connect schema
      val valSchema = record.valueSchema()
      val keySchema = record.keySchema()

      log.info("PROPERTIES:" + this.taskProperties)

     val avroData = new AvroData(new AvroDataConfig(this.taskProperties))

      //convert connect schema to avro schema
      //val avroValSchema = avroData.fromConnectSchema(valSchema)
      //val avroKeySchema = avroData.fromConnectSchema(keySchema)

      //convert to connect record to avro object
      val valObj: Object = avroData.fromConnectData(valSchema,record.value())
      val keyObj: Object = avroData.fromConnectData(keySchema,record.key())


      //4. Send producer record

      val producerRecord = new ProducerRecord[Object, Object](topic, keyObj, valObj)


      // Add headers
      for(header <- record.headers()){
        producerRecord.headers().add(new RecordHeader(header.key(), header.value().asInstanceOf[Array[Byte]]))
      }

      //producerRecord.headers().add(new RecordHeader("timestamp", System.currentTimeMillis().toString.getBytes()))
      this.kafkaProducer.send(producerRecord,callback)
    }
  }

  override def stop(): Unit = {
    log.info("Shutting down kafka producer!!")
    this.kafkaProducer.close()
  }

  override def version(): String = {
    new KafkaSinkConnector().version()
  }

  override def flush(currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    log.info("Flushing kafka producer for {}")
    this.kafkaProducer.flush()
  }

  def buildProperties: Properties = {
    val props: Properties = new Properties

    val hashMap = this.producerConfig.split(',').map(_.split("=")).map {case Array(k,v) => (k,v) }.toMap

    val javaMap: util.Map[_,_] = mapAsJavaMap(hashMap)

    props.putAll(javaMap)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers)
    props.setProperty("schema.registry.url", this.schemaRegistryUrl)

    props
  }


}

