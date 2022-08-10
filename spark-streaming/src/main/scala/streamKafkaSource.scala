import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object streamKafkaSource {

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Kafka Source")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Read Data From Kafka
    val initDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "quickstart")  //topic name
      .load()
      .select(col("value").cast("string"))

    // Perform word count on streaming DataFrame
    val wordCount = initDF
      .select(explode(split(col("value"), " ")).alias("words"))
      .groupBy("words")
      .count()

    //     Writing Data to Console
    wordCount
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()

    /*
    // Writing Data to Kafka Topic
    wordCount
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("update") // update
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testConsumer")
      .option("checkpointLocation", "checkpoint/kafka_checkpoint")
      .start()
      .awaitTermination()
    */

  }
}