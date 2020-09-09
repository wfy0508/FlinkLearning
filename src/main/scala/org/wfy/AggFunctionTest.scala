package org.wfy

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions._
import org.apache.flink.types.Row

/*
* @Author wfy
* @Date 2020/9/9 11:14
* org.wfy
*/

object AggFunctionTest {
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

    //5 创建一个聚合函数实例
    val avgTemp = new AvgTemp()

    //5.1 Table API调用
    val resultTable: Table = sensorTable
      .groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)

    //5.2 SQL调用
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)

    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        |id, avgTemp(temperature)
        |from sensor
        |group by id
        |""".stripMargin
    )

    // 转换成流打印输出
    resultTable.toRetractStream[(String, Double)].print("agg temp")
    resultSqlTable.toRetractStream[Row].print("agg temp sql")

    env.execute("aggFuc job test")
  }

  //定义AggregateFunction的累加器
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  //定义计算平均值函数
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }

}
