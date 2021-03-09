package com.reonomy.spark.udt

import com.reonomy.spark.udt.example.{DayOfWeekExample, UUIDExample}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.reonomy.udt

object Application{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("UUID-UDT")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
//    udt.registerAll()

    UUIDExample.plain(spark)
//    UUIDExample.tagged(spark)
//    DayOfWeekExample.plain(spark)
  }
}
