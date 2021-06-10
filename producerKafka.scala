import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord


object producerKafka extends App{
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("Kafka Producer")
      .getOrCreate()
      
      val props = new Properties
      props.put("bootstrap.servers","hostname:port")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      
      val producer = new KafkaProducer[String, String](props)
      
      val TOPIC = "topic_name"
      
      val data = "data from twitter"
  
    for (i <- 1 to data.length) {
      val record = new ProducerRecord(TOPIC, "key", data +"i")
      producer.send(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
    producer.send(record)
    producer.close()
}