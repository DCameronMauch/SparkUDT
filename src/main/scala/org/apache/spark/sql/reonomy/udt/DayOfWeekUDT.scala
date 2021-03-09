package org.apache.spark.sql.reonomy.udt

import com.reonomy.spark.udt.model.DayOfWeek
import org.apache.spark.sql.types.{DataType, StringType, UDTRegistration, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] class DayOfWeekUDT extends UserDefinedType[DayOfWeek]{
  def sqlType: DataType = StringType
  def serialize(dayOfWeek: DayOfWeek): Any = UTF8String.fromString(dayOfWeek.value)
  def deserialize(datum: Any): DayOfWeek = DayOfWeek(datum.asInstanceOf[UTF8String].toString)
  val userClass: Class[DayOfWeek] = classOf[DayOfWeek]
}

object DayOfWeekUDT {
  def register(): Unit = UDTRegistration.register(classOf[DayOfWeek].getName, classOf[DayOfWeekUDT].getName)
}
