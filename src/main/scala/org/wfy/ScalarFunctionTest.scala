package org.wfy

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions._
import org.apache.flink.types.Row

/*
* @Author wfy
* @Date 2020/9/6 21:48
* org.wfy
*/

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境
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

    // 4. 将流转化为表，直接定义时间字段(事件时间eventTime)
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime() as 'ts)

    // 5.  使用自定义hash函数，计算id的hash值
    val hashCode = new HashCode(1.23)

    // 5.1 Table API调用
    val resultTable: Table = sensorTable
      .select('id, 'ts, hashCode('id))

    //5.2 SQL调用方式，首先要注册表和函数
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, hashCode(id)
        |from sensor
        |""".stripMargin)

    // 6. 转换成流输出
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("UDF job test")

  }

  //定义一个求hash code d的标量函数
  class HashCode(factor: Double) extends ScalarFunction {
    def eval(value: String): Int = {
      (value.hashCode * factor).toInt
    }
  }

}
