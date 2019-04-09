package WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * User:ChengLiang
  * Date:2019/4/8
  * Time:17:50
  *
  *
  * cd /home/spark/spark-2.4.0-bin-hadoop2.7
  *
  * /home/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master local[*] --class WordCount.WordCount  /home/spark/study/book_2.11-0.1.jar
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    //      .setMaster("spark://192.168.1.21:4040")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("README.md")
    val words = textFile.flatMap(line => line.split(" "))
    val wordPairs = words.map(word => (word, 1))

    val wordCounts = wordPairs.reduceByKey((a, b) => a + b)
    println("wordCounts:" + wordCounts.collect().foreach(println))
  }
}
