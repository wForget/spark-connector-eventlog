package cn.wangz.spark.connector.eventlog

import java.util.{Locale, OptionalLong}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Cast, ExprUtils, Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.connector.eventlog.EventLogJSONOptionsInRead
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Statistics}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.execution.datasources.v2.json.{JsonPartitionReaderFactory, JsonScan}
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class EventLogScan(
    sparkSession: SparkSession,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  val eventLogDir = options.get(EventLogConfig.EVENT_LOG_DIR_KEY)

  val eventLogPartitioner: EventLogPartitioner = EventLogPartitioner(sparkSession, options)

  private lazy val allFiles = {
    if (partitionFilters.nonEmpty) {
      val predicate = partitionFilters.reduce(expressions.And)
      val boundPredicate = Predicate.createInterpreted(predicate.transform {
        case a: AttributeReference =>
          val index = readPartitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, readPartitionSchema(index).dataType, nullable = true)
      })
      eventLogPartitioner.getPartitions().filter { filePartition =>
        val partitionRow = partitionValues(filePartition)
        boundPredicate.eval(partitionRow)
      }
    } else {
      eventLogPartitioner.getPartitions()
    }
  }

  override protected def partitions: Seq[FilePartition] = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val splitFiles = allFiles.flatMap { filePartition =>
      PartitionedFileUtil.splitFiles(
        sparkSession = sparkSession,
        file = filePartition.file,
        filePath = filePartition.file.getPath,
        isSplitable = isSplitable(filePartition.file.getPath),
        maxSplitBytes = defaultMaxSplitBytes,
        partitionValues = partitionValues(filePartition)
      )
    }
    FilePartition.getFilePartitions(sparkSession, splitFiles, defaultMaxSplitBytes)
  }

  private val timeZoneId = options.asCaseSensitiveMap.asScala.toMap.getOrElse(
    DateTimeUtils.TIMEZONE_OPTION, sparkSession.sessionState.conf.sessionLocalTimeZone)

  private def partitionValues(filePartition: EventLogPartition): InternalRow = {
    InternalRow.fromSeq(readPartitionSchema.map { field =>
      val partValue = field.name match {
        case "dt" => filePartition.date
        case "hour" => filePartition.hour
        case "app_id" => filePartition.appId
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
  }

  // ---- copy from org.apache.spark.sql.execution.datasources.v2.json.JsonScan ---

  private val parsedOptions = new EventLogJSONOptionsInRead(
    CaseInsensitiveMap(options.asScala.toMap),
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def isSplitable(path: Path): Boolean = {
    JsonDataSource(parsedOptions).isSplitable && super.isSplitable(path)
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the json datasource is set multiLine mode"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (readDataSchema.length == 1 &&
      readDataSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new RuntimeException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
          "referenced columns only include the internal corrupt record column\n" +
          s"(named _corrupt_record by default). For example:\n" +
          "spark.read.schema(schema).json(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
          "and spark.read.schema(schema).json(file).select(\"_corrupt_record\").show().\n" +
          "Instead, you can cache or save the parsed results and then send the same query.\n" +
          "For example, val df = spark.read.schema(schema).json(file).cache() and then\n" +
          "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    JsonPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, pushedFilters)
  }

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def equals(obj: Any): Boolean = obj match {
    case j: JsonScan => super.equals(j) && dataSchema == j.dataSchema && options == j.options &&
      equivalentFilters(pushedFilters, j.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + pushedFilters.mkString("[", ", ", "]")
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val fileSizeInBytes = allFiles.map(_.file.getLen).sum
        val size = (compressionFactor * fileSizeInBytes).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def fileIndex: PartitioningAwareFileIndex = null

  override def getMetaData(): Map[String, String] = {
    Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "DataFilters" -> seqToString(dataFilters),
      "eventLogDir" -> eventLogDir)
  }

}
