package org.example

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import java.util
import java.util.{Collections, Properties, Random}
import scala.collection.JavaConversions._


class KafkaSourceTask extends SourceTask{

  private val log = LoggerFactory.getLogger(classOf[KafkaSourceTask])
  private val STRING_COLUMN = "string_column"
  private val NUMERIC_COLUMN = "numeric_column"
  private val BOOLEAN_COLUMN = "boolean_column"
  private val random:Random = new Random(System.currentTimeMillis)
  private val recordSchema:Schema = SchemaBuilder.struct().name("testschema")
    .field(STRING_COLUMN, Schema.STRING_SCHEMA).required()
    .field(NUMERIC_COLUMN, Schema.INT32_SCHEMA).required()
    .field(BOOLEAN_COLUMN, Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .build()
  var topic: String = ""

  var taskProperties: Properties = new Properties()
  private var streamOffset = 0
  /* lazy objects get evaluated at variable call */



  override def start(map: util.Map[String, String]): Unit = {
    log.info("INSIDE START: {}")
    map.foreach {case (key,value) => this.taskProperties.setProperty(key,value)}
    topic = map.get(KafkaSourceConfig.SOURCE_TOPIC)


  }

  override def poll(): util.List[SourceRecord] = {
    val records = new util.ArrayList[SourceRecord]
    streamOffset = streamOffset + 1

    val sourceOffset = Collections.singletonMap("position", streamOffset)
    print(createStruct(recordSchema))
    for(x <- recordSchema.fields()){
      print("schema fields")
      print(x.schema().toString)
      print(x.name())
    }

    //print(recordSchema.valueSchema())
    //print(recordSchema.schema()).toString
    //print(recordSchema)
 records.add(
    new SourceRecord(
      Collections.singletonMap("source", topic),
      Collections.singletonMap("offset", 0),
      topic, null, recordSchema, createStruct(recordSchema),
      recordSchema, createStruct(recordSchema)
    )
 )

    records
  }

  override def stop(): Unit = {

  }

  override def version(): String = {
    new KafkaSourceConnector().version()
  }


  private def createStruct(schema: Schema) = {
    val struct = new Struct(schema)

    struct.put(STRING_COLUMN, "asd")
    struct.put(NUMERIC_COLUMN, random.nextInt(1000))
    struct.put(BOOLEAN_COLUMN, random.nextBoolean)
    struct
  }


}


