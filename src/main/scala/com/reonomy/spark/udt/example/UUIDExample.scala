package com.reonomy.spark.udt.example

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import java.util.UUID
import shapeless.tag
import shapeless.tag.@@

object UUIDExample {
  case class EventPlain(id: Int, uuid: UUID)

  def plain(spark: SparkSession): Unit = {
    import spark.implicits._

    val uuid1: UUID = UUID.randomUUID()
    val uuid2: UUID = UUID.randomUUID()
    val uuid3: UUID = UUID.randomUUID()
    val uuid4: UUID = UUID.randomUUID()

    val ds1: Dataset[EventPlain] = Seq((1, uuid1), (2, uuid2), (3, uuid3), (4, uuid4)).toDF("id", "uuid").as[EventPlain]
    ds1.show(99, false)
    ds1.write.mode(SaveMode.Overwrite).parquet("uuid-plain")

    val ds2: Dataset[EventPlain] = spark.read.parquet("uuid-plain").as[EventPlain].orderBy($"id")
    ds2.show(99, false)
  }

  trait UserUUIDTag
  type UserUUID = UUID @@ UserUUIDTag
  case class EventTagged(id: Int, uuid: UserUUID)

  def tagged(spark: SparkSession): Unit = {
    import spark.implicits._

    val uuid1: UUID = tag[UserUUIDTag][UUID](UUID.randomUUID())
    val uuid2: UUID = tag[UserUUIDTag][UUID](UUID.randomUUID())
    val uuid3: UUID = tag[UserUUIDTag][UUID](UUID.randomUUID())
    val uuid4: UUID = tag[UserUUIDTag][UUID](UUID.randomUUID())

    val ds1: Dataset[EventTagged] = Seq((1, uuid1), (2, uuid2), (3, uuid3), (4, uuid4)).toDF("id", "uuid").as[EventTagged]
    ds1.show(99, false)
    ds1.write.mode(SaveMode.Overwrite).parquet("uuid-tagged")

    val ds2: Dataset[EventTagged] = spark.read.parquet("uuid-tagged").as[EventTagged].orderBy($"id")
    ds2.show(99, false)
  }
}
