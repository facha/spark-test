import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

//import org.apache.spark._
//import org.apache.spark.SparkContext._

object SparkKafkaConsumer{
  def main(args: Array[String]) {
    val numThreads = 2
    val zkQuorum = "ip-172-31-43-82.us-west-2.compute.internal:2181"
    val group = "gr1"
    val topics = "dfadeyev"
    val sparkConf = new SparkConf().setAppName("SparkKafkaConsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
