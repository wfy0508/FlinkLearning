package org.wfy

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/*
* @Author wfy
* @Date 2020/9/1 18:06
* org.wfy
*/

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 从文件读取，转换成流
    val inputStream = env.readTextFile("D:\\Learning\\Workspace\\FlinkLearning\\src\\main\\resources\\sensor.txt")

    // 3. map成样例类
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    // 4. 将流转化为表，直接定义时间字段(处理时间processTime)
    // val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime())

    // 4. 将流转化为表，直接定义时间字段(事件时间eventTime)
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime() as 'ts)


    // 5.1 Table API实现
    // 5.1.1 Group Window操作
    val resultTable: Table = sensorTable
      .window(Tumble over 10.seconds() on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count(), 'tw.end)

    // 打印输出
    //resultTable.toRetractStream[Row].print("agg")

    // 5.1.2 Over Window操作
    val overResultTable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)


    // 5.2 SQL实现
    // Group Windows
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id), hop_end(ts, interval '4' second, interval '10' second)
        |from sensor
        |group by id, hop(ts, interval '4' second, interval '10' second)
        |""".stripMargin
    ) //hop_end, hop定义的是滑动窗口

    // Over Windows
    val orderSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, count(id) over w, avg(temperature) over w
        |from sensor
        |window w as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
        |""".stripMargin
    )

    // 打印输出
    orderSqlTable.toRetractStream[Row].print("agg sql")

    env.execute()
  }
}
