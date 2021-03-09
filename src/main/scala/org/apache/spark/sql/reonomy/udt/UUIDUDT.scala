package org.apache.spark.sql.reonomy.udt

import org.apache.spark.sql.types.{DataType, StringType, UDTRegistration, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String
import java.util.UUID

private[sql] class UUIDUDT extends UserDefinedType[UUID]{
  def sqlType: DataType = StringType
  def serialize(uuid: UUID): Any = UTF8String.fromString(uuid.toString)
  def deserialize(datum: Any): UUID = UUID.fromString(datum.asInstanceOf[UTF8String].toString)
  val userClass: Class[UUID] = classOf[UUID]
}

object UUIDUDT {
  def register(): Unit = UDTRegistration.register(classOf[UUID].getName, classOf[UUIDUDT].getName)
}
