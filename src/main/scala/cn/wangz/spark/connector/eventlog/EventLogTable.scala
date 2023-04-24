package cn.wangz.spark.connector.eventlog

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.eventlog.EventLogJSONOptionsInRead
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters._

case class EventLogTable(name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap) extends Table with SupportsRead with SupportsPartitionManagement with Logging {

  // e.g., a INT, b STRING.
  private val customSchema = options.get(EventLogConfig.EVENT_LOG_SCHEMA_KEY) match {
    case schema: String if StringUtils.isNoneBlank(schema) =>
      Some(StructType.fromDDL(schema))
    case _ => None
  }

  private val inferSchema: Option[StructType] = {
    val eventLogDir = options.get(EventLogConfig.EVENT_LOG_DIR_KEY)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val path = new Path(eventLogDir)
    val fs = path.getFileSystem(hadoopConf)
    val files = fs.listStatus(path)
      .filter(!_.getPath.getName.endsWith(".inprogress"))
      .filter(_.getLen <= 512 * 1024 * 1024)
      .sortBy(-_.getAccessTime)
      .take(10) // TODO just infer 10 files
    val parsedOptions = new EventLogJSONOptionsInRead(
      CaseInsensitiveMap(options.asScala.toMap),
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    TextInputJsonDataSource.inferSchema(sparkSession, files, parsedOptions)
  }

//  private val defaultSchema = StructType(
//    StructField("Event", StringType, true) ::
//      StructField("Job ID", StringType, true) ::
//      StructField("Completion Time", StringType, true) ::
//      // `Job Result` Struct<`Result` String, `Exception` Struct<`Message` String, `Stack Trace` String>>
//      StructField("Job Result", StructType(
//        StructField("Result", StringType, true) ::
//          StructField("Exception",
//            StructType(
//              StructField("Message", StringType, true) ::
//                StructField("Stack Trace", StringType, true) :: Nil)
//            , true
//          ) :: Nil)
//        , true
//      ) ::
//      Nil
//  )

  lazy val dataSchema: StructType = customSchema.orElse(inferSchema).getOrElse {
    throw new RuntimeException("Unable to infer schema for json. It must be specified manually.")
  }

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
