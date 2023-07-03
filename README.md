# kafka-connect-topic-replicator


{
     "name": "kafka-sink-raw2data-milan-enrich1",
    "connector.class": "org.example.KafkaSinkConnector",
    "topics": "milan-data1",
    "destination.topic" : "milan-enriched",
    "destination.bootstrap.servers" : "broker:29092",
    "destination.key.serializer": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "destination.value.serializer": "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "destination.schema.registry.url": "http://schema-registry:8089",
    "kafka.producer.configs": "key.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer,value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer,acks=all,retries=10",
    
    
    "tasks.max": "1",
    "header.converter.schema.registry.url": "http://schema-registry:8089",
    "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8089",
    "key.converter.schema.registry.url": "http://schema-registry:8089"

 


}
