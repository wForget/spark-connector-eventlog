## Spark SQL 读取 EventLog

### 一、背景

此项目用于 Spark SQL 读取分析 Spark Event Log。

Spark Event Log 是以 JSON 文件保存的，所以我们可以直接通过 Spark SQL 操作 JSON File 方式读取 Spark Event Log 文件。

由于 Spark Event Log 的所有文件保存在同一个目录下面，当我们需要聚合分析时，不得不读取全量的数据。

所以此项目主要通过实现一个 EventLogCatalog，为一个 application 文件添加 dt、hour、app_id 的分区，方便根据分区过滤并进行 SQL 处理。

### 二、打包配置

#### 打包并添加到 Spark 依赖

```
mvn clean package -DskipTests
```

将打完的包，复制到 $SPARK_HOME/jars 中，或通过 add jar 引入：

```
add jar hdfs://XXX/spark-connector-eventlog-X.X-SNAPSHOT.jar;
```

#### 配置 Catalog

```
spark.sql.catalog.eventlog=cn.wangz.spark.connector.eventlog.EventLogCatalog
spark.sql.catalog.eventlog.eventLogDir=hdfs://namenode01/tmp/spark/logs
```

### 三、使用分析 EventLog

#### 表名

默认为：`spark_event_log`

可通过 `tableName` 属性配置，如：

```
spark.sql.catalog.eventlog.tableName=spark_event_log_new
```

#### 分区

| 字段名 | 类型 |
| --- | --- |
| dt | string |
| hour | string |
| app_id | string |

#### Schema

默认：自动读取最新的 10 个 Event Log 文件进行推断。

可通过 `schema` 属性配置，如：
```
spark.sql.catalog.eventlog.schema="Event String, `Job Result` Struct<Result: String, Exception: String>"
```

#### 读取 EventLog

```
// 可通过设置 maxPartitionBytes，控制每个 task 处理的数据量
set spark.sql.files.maxPartitionBytes=1g

// 查询表
show tables in eventlog

// 查询分区
show partitions eventlog.spark_event_log

// 查询 EventLog
select * from eventlog.spark_event_log where dt = '2022-10-26'
```

### 四、扩展 UDF

#### SparkPlanNodesUDF

获取 sparkPlanInfo 所有节点。

Spark Event (SparkListenerSQLAdaptiveExecutionUpdate/SparkListenerSQLExecutionStart) 中 SparkPlanInfo 是通过嵌套的树型对象存储，很难通过 SQL Struct 类型表示，所以默认将 sparkPlanInfo 字段默认设置为 String 类型，展示原始 json 字符串。

提供一个 `SparkPlanNodesUDF` UDF 来处理 sparkPlanInfo json 字符串，获取所有 plan 节点。

使用：

```
CREATE TEMPORARY FUNCTION spark_plan_nodes AS 'cn.wangz.spark.connector.eventlog.udf.SparkPlanNodesUDF';
```

### 五、使用案例

1. 失败信息查询

    ```sql
    set  spark.sql.catalog.eventlog.schema=`Event` String,`Job ID` String,`Completion Time` String,`Job Result` Struct<`Result`: String, `Exception`: Struct<`Message`: String, `Stack Trace`: String>>;

    select app_id, substring_index(`Job Result`.`Exception`.`Message`, '\n', 1) from eventlog.spark_event_log where dt = '2022-11-03' and `Job Result`.`Result` = 'JobFailed' limit 10;
    ```

2. 倾斜任务查询

    ```sql
    set spark.sql.catalog.eventlog.schema=`Event` String,`Stage ID` Int,`Stage Attempt ID` Int,`Task Type` String,`Task End Reason` String,`Task Info` String,`Task Executor Metrics` String,`Task Metrics` Struct<`Executor Run Time`: Long>;

    select * from (select app_id, `Stage ID`, percentile(`Task Metrics`.`Executor Run Time`, 0.75) AS P75, max(`Task Metrics`.`Executor Run Time`) as MAX from eventlog.spark_event_log where dt = '2022-11-08' and hour=10 and `Event` = 'SparkListenerTaskEnd' and `Task Metrics`.`Executor Run Time` is not null group by app_id, `Stage ID`) t order by MAX desc limit 100;
    ```

3. Task 序列化时间过长

    ```sql
    select app_id, `Stage ID`, `Stage Attempt ID`, `Task Info`.`Task ID`, `Task Metrics`.`Executor Deserialize Time`
    from eventlog.spark_event_log where dt = '2023-04-24' and Event = 'SparkListenerTaskEnd'
    order by `Task Metrics`.`Executor Deserialize Time` desc limit 50;
    ```

4. 查询 Kyuubi 任务

    ```sql
    select distinct app_id from eventlog.spark_event_log where dt = '2023-06-13' and `Event` = 'SparkListenerEnvironmentUpdate' and `Spark Properties`['spark.yarn.tags'] = 'KYUUBI' limit 10;
    ```
   
5. Kyuubi 倾斜任务

    ```sql
    with kyuubi_apps as (select distinct app_id
      from eventlog.spark_event_log
      where dt = '2023-06-13'
      and `Event` = 'SparkListenerEnvironmentUpdate'
      and `Spark Properties`['spark.yarn.tags'] = 'KYUUBI'),
    skew_apps as (select app_id,
      `Stage ID`,
      percentile(`Task Metrics`.`Executor Run Time`, 0.75) AS P75,
      max(`Task Metrics`.`Executor Run Time`)              as MAX
      from eventlog.spark_event_log
      where dt = '2023-06-13'
      and `Event` = 'SparkListenerTaskEnd'
      and `Task Metrics`.`Executor Run Time` is not null
      group by app_id, `Stage ID`)
    select * from kyuubi_apps t1 join skew_apps t2 on t1.app_id = t2.app_id order by MAX desc limit 100;
    ```