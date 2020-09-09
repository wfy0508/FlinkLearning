package org.wfy

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/*
* @Author wfy
* @Date 2020/9/9 17:47
* org.wfy
*/

object TableAggFunctionTest {
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

    //创建一个表聚合函数实例
    val top2Temp = new Top2Temp()

    //Table API
    val resultTable: Table = sensorTable
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("top2Temp", top2Temp)

    //SQL调用
    val resultSqlResult: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from sensor
        |""".stripMargin)

    // 转换成流打印输出
    resultTable.toRetractStream[(String, Double, Int)].print("agg temp")

    env.execute()
  }

  //定义一个累加器
  class Top2TempAcc {
    var highestTemp: Double = Int.MinValue
    var secondHighestTemp: Double = Int.MinValue
  }

  //定义一个TableAggregateFunction
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    //定义更新累加器方法
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    //定义输出结果方法
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }

}
