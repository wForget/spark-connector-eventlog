package cn.wangz.spark.connector.eventlog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class EventLogScanBuilder(sparkSession: SparkSession,
    partitionSchema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap) extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters  {
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  protected val supportsNestedSchemaPruning = false
  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // [SPARK-30107] While `requiredSchema` might have pruned nested columns,
    // the actual data schema of this scan is determined in `readDataSchema`.
    // File formats that don't support nested schema pruning,
    // use `requiredSchema` as a reference and prune only top-level columns.
    this.requiredSchema = requiredSchema
  }

  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val schema = if (supportsNestedSchemaPruning) requiredSchema else dataSchema
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  protected def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  private val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (sparkSession.sessionState.conf.jsonFilterPushDown) {
      _pushedFilters = StructFilters.pushedFilters(filters, dataSchema)
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    EventLogScan(
      sparkSession,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      pushedFilters())
  }
}
