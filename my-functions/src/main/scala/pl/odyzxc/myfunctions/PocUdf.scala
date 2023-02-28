package pl.odyzxc.myfunctions

import org.apache.spark.sql.api.java.UDF1

class PocUdf extends UDF1[String, String] {
  override def call(value: String): String = {
    value + "dupa"
  }
}
