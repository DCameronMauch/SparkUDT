package com.reonomy.spark.udt.model

sealed abstract class DayOfWeek(val value: String) {
  self =>
  lazy val isWeekDay: Boolean = DayOfWeek.weekDay.contains(self)
  lazy val isWeekEnd: Boolean = DayOfWeek.weekEnd.contains(self)
}

object DayOfWeek {
  final case object Monday extends DayOfWeek("Monday")
  final case object Tuesday extends DayOfWeek("Tuesday")
  final case object Wednesday extends DayOfWeek("Wednesday")
  final case object Thursday extends DayOfWeek("Thursday")
  final case object Friday extends DayOfWeek("Friday")
  final case object Saturday extends DayOfWeek("Saturday")
  final case object Sunday extends DayOfWeek("Sunday")

  val weekDay: Set[DayOfWeek] = Set(Monday, Tuesday, Wednesday, Thursday, Friday)
  val weekEnd: Set[DayOfWeek] = Set(Saturday, Sunday)
  val members: Set[DayOfWeek] = weekDay ++ weekEnd
  def apply(value: String): DayOfWeek = members.find(_.value == value).orNull
}
