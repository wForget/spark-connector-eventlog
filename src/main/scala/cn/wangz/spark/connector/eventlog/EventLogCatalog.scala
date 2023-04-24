package cn.wangz.spark.connector.eventlog

import java.util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import cn.wangz.spark.connector.eventlog.EventLogConfig._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import java.util.regex.Pattern

class EventLogCatalog extends TableCatalog {

  private var _name: String = _
  private var _options: CaseInsensitiveStringMap = _
  override def name(): String = _name

  private var eventLogTableName: String = "spark_event_log"

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
    _options = options
    if (options.containsKey(EVENT_LOG_TABLE_NAME_KEY)) {
      eventLogTableName = options.get(EVENT_LOG_TABLE_NAME_KEY)
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = Array(Identifier.of(namespace, eventLogTableName))

  override def loadTable(ident: Identifier): Table = {
    if (eventLogTableName.equalsIgnoreCase(ident.name())) {
      val spark = SparkSession.getActiveSession.get
      EventLogTable(eventLogTableName, spark, runtimeCatalogOptions(spark))
    } else {
      throw new NoSuchTableException(ident)
    }
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    throw new UnsupportedOperationException
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException
  }

  private def runtimeCatalogOptions(spark: SparkSession): CaseInsensitiveStringMap = {
    val prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)")
    val options = new util.HashMap[String, String]
    options.putAll(_options)
    spark.conf.getAll.foreach {
      case (key, value) =>
        val matcher = prefix.matcher(key)
        if (matcher.matches && matcher.groupCount > 0) options.put(matcher.group(1), value)
    }
    new CaseInsensitiveStringMap(options)
  }

}
