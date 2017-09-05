import org.apache.spark._
import org.apache.spark.streaming._

object NetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
