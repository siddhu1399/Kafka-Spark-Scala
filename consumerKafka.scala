import org.apache.spark.sql.SparkSession

object consumerKafka extends App {
/* creating a spark session */
    
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("KafkaConsumer")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

/* to establish connection to Kafka , subscribe to a topic */
    
    val ds = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host_name:port")
      .option("subscribe", "topic_name")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/")
    
/* creating a spark dataframe to store messages in kafka topic into hive table */
    
    val df = ds
      .write
      .format("hive")
      .option("kafka.bootstrap.servers", "host_name:port")
      .option("topic", "topic_name")
      .saveAsTable("db.table_name")
  }

