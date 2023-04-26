package cn.wangz.spark.connector.eventlog.udf

import cn.wangz.spark.connector.eventlog.udf.SparkPlanNodesUDF.mapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory._
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo

import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._


class SparkPlanNodesUDF extends GenericUDF with Logging {

  private val inputOI: JavaStringObjectInspector = javaStringObjectInspector

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 1)
      throw new UDFArgumentException(s"The arguments is invalid, usage:${getDisplayString(null)} .")

    if (arguments(0).getCategory() != ObjectInspector.Category.PRIMITIVE
      || !PrimitiveObjectInspector.PrimitiveCategory.STRING.equals(arguments(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory())) {
      throw new UDFArgumentException(s"The arguments is invalid, please use a string type argument.")
    }

    val fileNames: util.List[String] = util.Arrays.asList("id", "nodeName", "simpleString", "metadata", "metrics")
    val ois: util.List[ObjectInspector] = util.Arrays.asList(
      javaIntObjectInspector,
      javaStringObjectInspector,
      javaStringObjectInspector,
      ObjectInspectorFactory.getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
      ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(
          util.Arrays.asList("name", "accumulatorId", "metricType"),
          util.Arrays.asList(javaStringObjectInspector, javaLongObjectInspector, javaStringObjectInspector)
        )
      )
    )
    ObjectInspectorFactory.getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardStructObjectInspector(fileNames, ois)
    )
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val argument = inputOI.getPrimitiveJavaObject(arguments(0).get())
    logInfo(s"argument: $argument")
    if (argument != null) {
      val sparkPlanInfo = mapper.readValue[SparkPlanInfo](argument, classOf[SparkPlanInfo])
      sparkPlanInfoNodes(sparkPlanInfo, new AtomicInteger(0))
    } else {
      null
    }
  }

  private def sqlMetricInfoNode(sqlMetricInfo: SQLMetricInfo): util.List[Any] = {
    util.Arrays.asList(sqlMetricInfo.name, sqlMetricInfo.accumulatorId, sqlMetricInfo.metricType)
  }

  private def sparkPlanInfoNode(sparkPlanInfo: SparkPlanInfo, idGenerator: AtomicInteger): util.List[Any] = {
    util.Arrays.asList(idGenerator.getAndIncrement(),
      sparkPlanInfo.nodeName,
      sparkPlanInfo.simpleString,
      sparkPlanInfo.metadata.asJava,
      sparkPlanInfo.metrics.map(sqlMetricInfoNode(_)).toList.asJava)
  }

  private def sparkPlanInfoNodes(sparkPlanInfo: SparkPlanInfo, idGenerator: AtomicInteger): util.List[util.List[Any]] = {
    val nodes: util.List[util.List[Any]] = new util.ArrayList[util.List[Any]]()
    nodes.add(sparkPlanInfoNode(sparkPlanInfo, idGenerator))
    val children = sparkPlanInfo.children
    if (!children.isEmpty) {
      children.foreach(n => nodes.addAll(sparkPlanInfoNodes(n, idGenerator)))
    }
    nodes
  }

  override def getDisplayString(children: Array[String]): String =
    "spark_plan_nodes(sparkPlanInfo: String): Array[SparkPlanGraphNode]"
}

object SparkPlanNodesUDF {

  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    val value =
      """
        |{"nodeName":"AdaptiveSparkPlan","simpleString":"AdaptiveSparkPlan isFinalPlan=false","children":[{"nodeName":"HashAggregate","simpleString":"HashAggregate(keys=[type#4, agenttype#0], functions=[count(1)])","children":[{"nodeName":"ShuffleQueryStage","simpleString":"ShuffleQueryStage 0","children":[{"nodeName":"Exchange","simpleString":"Exchange hashpartitioning(type#4, agenttype#0, 200), ENSURE_REQUIREMENTS, [id=#28]","children":[{"nodeName":"WholeStageCodegen (1)","simpleString":"WholeStageCodegen (1)","children":[{"nodeName":"HashAggregate","simpleString":"HashAggregate(keys=[type#4, agenttype#0], functions=[partial_count(1)])","children":[{"nodeName":"Project","simpleString":"Project [agenttype#0, type#4]","children":[{"nodeName":"Scan ExistingRDD","simpleString":"Scan ExistingRDD[agenttype#0,loginType#1,snsType#2,subType#3,type#4,uri#5]","children":[],"metadata":{},"metrics":[{"name":"number of output rows","accumulatorId":21,"metricType":"sum"}]}],"metadata":{},"metrics":[]}],"metadata":{},"metrics":[{"name":"spill size","accumulatorId":18,"metricType":"size"},{"name":"time in aggregation build","accumulatorId":19,"metricType":"timing"},{"name":"peak memory","accumulatorId":17,"metricType":"size"},{"name":"number of output rows","accumulatorId":16,"metricType":"sum"},{"name":"avg hash probe bucket list iters","accumulatorId":20,"metricType":"average"}]}],"metadata":{},"metrics":[{"name":"duration","accumulatorId":38,"metricType":"timing"}]}],"metadata":{},"metrics":[{"name":"shuffle records written","accumulatorId":36,"metricType":"sum"},{"name":"shuffle write time","accumulatorId":37,"metricType":"nsTiming"},{"name":"records read","accumulatorId":34,"metricType":"sum"},{"name":"local bytes read","accumulatorId":32,"metricType":"size"},{"name":"fetch wait time","accumulatorId":33,"metricType":"timing"},{"name":"remote bytes read","accumulatorId":30,"metricType":"size"},{"name":"local blocks read","accumulatorId":29,"metricType":"sum"},{"name":"remote blocks read","accumulatorId":28,"metricType":"sum"},{"name":"data size","accumulatorId":27,"metricType":"size"},{"name":"remote bytes read to disk","accumulatorId":31,"metricType":"size"},{"name":"shuffle bytes written","accumulatorId":35,"metricType":"size"}]}],"metadata":{},"metrics":[]}],"metadata":{},"metrics":[{"name":"spill size","accumulatorId":24,"metricType":"size"},{"name":"time in aggregation build","accumulatorId":25,"metricType":"timing"},{"name":"peak memory","accumulatorId":23,"metricType":"size"},{"name":"number of output rows","accumulatorId":22,"metricType":"sum"},{"name":"avg hash probe bucket list iters","accumulatorId":26,"metricType":"average"}]}],"metadata":{},"metrics":[]}
        |""".stripMargin
    val sparkPlanInfo = mapper.readValue[SparkPlanInfo](value, classOf[SparkPlanInfo])
    println(sparkPlanInfo)
  }

}
