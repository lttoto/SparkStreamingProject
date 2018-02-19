package com.lt.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/18.
  * SpreakStreaming处理文件系统
  */
object FileWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.textFileStream("fileLocation must be DIR")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
