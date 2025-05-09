package cn.wangz.spark.connector.eventlog

import cn.wangz.spark.connector.eventlog.EventLogPartitioner._

import java.util.{Date, TimeZone}
import org.apache.commons.lang3.time.DateFormatUtils

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.mutable.ListBuffer

class EventLogPartitioner(sparkSession: SparkSession, options: CaseInsensitiveStringMap) extends Logging {

  val eventLogDir = options.get(EventLogConfig.EVENT_LOG_DIR_KEY)

  def getPartitions(): Seq[EventLogPartition] = {
    val path = new Path(eventLogDir)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val fs = path.getFileSystem(hadoopConf)
    val files = fs.listStatus(path)
      .filter(_.getLen <= 1024 * 1024 * 1024) // TODO file size less than 1GB
    files.flatMap(getPartitionFromFileStatus)
  }

  private def getPartitionFromFileStatus(fileStatus: FileStatus): Option[EventLogPartition] = {
    val fileName = fileStatus.getPath.getName
    val matcher = appIdPattern.findFirstMatchIn(fileName)
    if (!matcher.isEmpty) {
      val appId = matcher.get.group(1)
      val time = new Date(fileStatus.getModificationTime)
      val date = DateFormatUtils.format(time, "yyyy-MM-dd", TimeZone.getDefault)
      val hour = DateFormatUtils.format(time, "HH", TimeZone.getDefault)
      Some(EventLogPartition(date, hour, appId, fileStatus))
    } else {
      None
    }
  }

}

object EventLogPartitioner {
  def apply(sparkSession: SparkSession, options: CaseInsensitiveStringMap): EventLogPartitioner = new EventLogPartitioner(sparkSession, options)

  private val appIdPattern = "^(application_\\d+_\\d+)(_\\d+)?$".r
}


case class EventLogPartition(date: String, hour: String, appId: String, file: FileStatus)
