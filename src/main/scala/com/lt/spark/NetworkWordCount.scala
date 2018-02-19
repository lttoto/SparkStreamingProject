package com.lt.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/18.
  *
  * SparkStream处理Socket数据
  * nc测试
  */
object NetworkWordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
