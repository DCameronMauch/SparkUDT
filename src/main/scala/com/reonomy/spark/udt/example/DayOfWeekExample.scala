package com.reonomy.spark.udt.example

import com.reonomy.spark.udt.model.DayOfWeek
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DayOfWeekExample {
  case class EventPlain(id: Int, dayOfWeek: DayOfWeek)

  def plain(spark: SparkSession): Unit = {
    import spark.implicits._

    val dayOfWeek1: DayOfWeek = DayOfWeek.Monday
    val dayOfWeek2: DayOfWeek = DayOfWeek.Saturday
    val dayOfWeek3: DayOfWeek = DayOfWeek.Tuesday
    val dayOfWeek4: DayOfWeek = DayOfWeek.Sunday

    val ds1: Dataset[EventPlain] = Seq((1, dayOfWeek1), (2, dayOfWeek2), (3, dayOfWeek3), (4, dayOfWeek4)).toDF("id", "dayOfWeek").as[EventPlain]
    ds1.printSchema()
    ds1.show(99, false)
    ds1.write.mode(SaveMode.Overwrite).parquet("day-of-week-plain")

    val ds2: Dataset[EventPlain] = spark.read.parquet("day-of-week-plain").as[EventPlain].orderBy($"id")
    ds2.printSchema()
    ds2.show(99, false)
    ds2.filter(e => e.dayOfWeek.isWeekDay).show(99, false)
  }
}
