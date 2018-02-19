package com.lt.spark.project.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by taoshiliu on 2018/2/19.
  */
object DataUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /*def main(args: Array[String]) {
    println(parseToMinute("2017-10-22 14:46:01"))
  }*/

}
