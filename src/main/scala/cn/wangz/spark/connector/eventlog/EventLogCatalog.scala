package cn.wangz.spark.connector.eventlog

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import cn.wangz.spark.connector.eventlog.EventLogConfig._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

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
      EventLogTable(eventLogTableName, SparkSession.getActiveSession.get, _options)
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

}
