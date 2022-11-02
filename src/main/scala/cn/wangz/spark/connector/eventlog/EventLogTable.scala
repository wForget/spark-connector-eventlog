package cn.wangz.spark.connector.eventlog

import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConverters._
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

case class EventLogTable(name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap) extends Table with SupportsRead with SupportsPartitionManagement{

  // e.g., a INT, b STRING.
  private val customSchema = options.get(EventLogConfig.EVENT_LOG_SCHEMA_KEY) match {
    case schema: String if StringUtils.isNoneBlank(schema) =>
      Some(StructType.fromDDL(schema))
    case _ => None
  }

  private val defaultSchema = StructType(
    StructField("Event", StringType, true) ::
      StructField("Job ID", StringType, true) ::
      StructField("Completion Time", StringType, true) ::
      // `Job Result` Struct<`Result` String, `Exception` Struct<`Message` String, `Stack Trace` String>>
      StructField("Job Result", StructType(
        StructField("Result", StringType, true) ::
          StructField("Exception",
            StructType(
              StructField("Message", StringType, true) ::
                StructField("Stack Trace", StringType, true) :: Nil)
            , true
          ) :: Nil)
        , true
      ) ::
      Nil
  )

  lazy val dataSchema: StructType = customSchema.getOrElse(defaultSchema)

  lazy val partitionSchema: StructType = StructType(
    StructField("dt", StringType, true) ::
      StructField("hour", StringType, false) ::
      StructField("app_id", StringType, false) :: Nil)

  override def schema(): StructType = {
    StructType(dataSchema.fields ++ partitionSchema.fields)
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(scanOpt: CaseInsensitiveStringMap): ScanBuilder = {
    val newOptions = options.asScala ++ scanOpt.asScala
    new EventLogScanBuilder(sparkSession, partitionSchema, dataSchema, new CaseInsensitiveStringMap(newOptions.asJava))
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    throw new UnsupportedOperationException
  }

  override def replacePartitionMetadata(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    throw new UnsupportedOperationException
  }

  private val eventLogPartitioner: EventLogPartitioner = EventLogPartitioner(sparkSession, options)
  override def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] = {
    eventLogPartitioner.getPartitions().filter(filePartition => {
      if (!names.isEmpty) {
        names.zipWithIndex.map { case(name, index) =>
          val filterValue = ident.getString(index)
          val value = name match {
            case "dt" => filePartition.date
            case "hour" => filePartition.hour
            case "app_id" => filePartition.appId
          }
          value == filterValue
        }.reduce(_ && _)
      } else {
        true
      }
    }).map(f =>
      InternalRow(UTF8String.fromString(f.date),
        UTF8String.fromString(f.hour),
        UTF8String.fromString(f.appId))
    ).toArray
  }
}
