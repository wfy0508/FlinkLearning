package org.wfy

import com.sun.tools.jdeprscan.CSV
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/*
* @Author wfy
* @Date 2020/9/2 15:03
* org.wfy
*/

object TableApiTest extends App {
  // 创建流处理执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // 创建表执行环境
  val tableEnv = StreamTableEnvironment.create(env)

  // 从文件读取数据
  val filePath = "D:\\Learning\\Workspace\\FlinkLearning\\src\\main\\resources\\sensor.txt"

  // 连接文件系统，读取文件
  tableEnv.connect(new FileSystem().path(filePath))
    .withFormat(new Csv())
    .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
      .rowtime(new Rowtime()
        .timestampsFromField("timestamp")
        .watermarksPeriodicBounded(1000))
    )
    .createTemporaryTable("inputTable")

  // 在DDL语言中定义事件时间
  val sinkDDL: String =
    """
      |create table dataTable(
      |id varchar(20) not null,
      |ts bigint,
      |temperature double,
      |rt as TO_TIMESTAMP(FROM_UNIXTIME(ts)),
      |watermark for rt as rt - interval '1' seconds
      |) with (
      |'connector.type' = 'filesystem',
      |'connector.path' = '/sensor.txt',
      |'from.type' = 'csv'
      |)
      |""".stripMargin

  tableEnv.sqlUpdate(sinkDDL)
}
