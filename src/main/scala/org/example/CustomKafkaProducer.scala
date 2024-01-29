package org.example

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties
import java.util
import scala.collection.JavaConversions._

object CustomKafkaProducer {
  var counter = 0
  var kafkaProducer: KafkaProducer[Object, Object] = null

  def registerKafkaProducerNew(producerConfig: String, bootstrapServers: String, schemaRegistryUrl: String):Unit = {

    val props: Properties = new Properties
    val hashMap = producerConfig.split(',').map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
    val javaMap: util.Map[_, _] = mapAsJavaMap(hashMap)
    props.putAll(javaMap)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty("schema.registry.url", schemaRegistryUrl)

    counter = counter + 1
    println("CALLING FUNCTION CALL: " + counter.toString)

    this.kafkaProducer  = new KafkaProducer(props)

  }

  def getKafkaProducerInstance: KafkaProducer[Object, Object]= {
    println("CALLING INSIDE PRODUCER CALL: " + counter.toString)
    this.kafkaProducer
  }

}
