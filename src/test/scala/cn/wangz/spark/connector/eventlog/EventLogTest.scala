package cn.wangz.spark.connector.eventlog

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EventLogTest {

  val eventLogDir = "hdfs://namenode01/tmp/spark/logs"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.eventlog", classOf[EventLogCatalog].getName)
      .set("spark.sql.catalog.eventlog.eventLogDir", eventLogDir)
      .set("spark.sql.catalog.eventlog.schema", "Event String, `Job Result` Struct<Result: String, Exception: String>")

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate();

    sparkSession.sql("show tables in eventlog").show()

    sparkSession.sql("show partitions eventlog.spark_event_log").show()

    sparkSession.sql("select * from eventlog.spark_event_log where dt = '2022-10-26'").show()
  }

}
