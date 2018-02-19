package com.lt.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  */
object WindowWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1))
    val windowedWordCounts = result.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2,Seconds(30),Seconds(10))

    ssc.start()
    ssc.awaitTermination()
  }
}
