package cn.wangz.spark.connector.eventlog

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File

object EventLogTest {

  private val eventLogDir = new File("src/test/resources/spark-events").toURI.toString

  def main(args: Array[String]): Unit = {
    println(eventLogDir)
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.eventlog", classOf[EventLogCatalog].getName)
      .set("spark.sql.catalog.eventlog.eventLogDir", eventLogDir)

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    sparkSession.sql("show tables in eventlog").show()

    sparkSession.sql("show partitions eventlog.spark_event_log").show()

    sparkSession.sql("select * from eventlog.spark_event_log").show()
  }

}
