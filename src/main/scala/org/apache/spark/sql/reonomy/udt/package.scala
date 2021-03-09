package org.apache.spark.sql.reonomy

package object udt {
  def registerAll(): Unit = {
    udt.DayOfWeekUDT.register()
    udt.UUIDUDT.register()
  }
}
