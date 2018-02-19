package com.lt.spark



import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  * SparkStreaming 整合SparkSQL
  */
object SqlNetWorkCount {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD{
      (rdd: RDD[String], time: Time) =>
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        // Convert RDD[String] to RDD[case class] to DataFrame
        val wordsDataFrame = rdd.map(w => Record(w)).toDF()

        // Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        // Do word count on table using SQL and print it
        val wordCountsDataFrame =
          spark.sql("select word, count(*) as total from words group by word")
        println(s"========= $time =========")
        wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  case class Record(word: String)


  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
