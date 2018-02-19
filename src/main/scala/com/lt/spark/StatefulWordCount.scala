package com.lt.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/18.
  * SparkStreaming实现有状态的统计
  */
object StatefulWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //使用statful算子，必须使用checkpoint
    //生产环境中设置到HDFS目录中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1))
    val stat = result.updateStateByKey[Int](updateFunction _)
    stat.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
