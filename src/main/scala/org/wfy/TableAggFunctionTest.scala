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
    // 0 创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 2 从文件读取数据
    val inputData = env.readTextFile("D:\\Learning\\Workspace\\FlinkLearning\\src\\main\\resources\\sensor.txt")

    // 3 将数据源转换为DataStream
    val dataStream = inputData
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    // 4 将流转换为表，并定义时间字段
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime() as 'ts)

    // 创建表聚合函数实例
    val top2Temp = new Top2Temp()

    // 5 Table API调用
    val resultTable = sensorTable
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    // 6 转换为流打印输出
    resultTable.toRetractStream[(String, Double, Int)].print("agg temp")

    env.execute("table agg function job test")
  }


  // 定义一个累加器类
  class Top2TempAcc {
    var highestTemp: Double = Int.MinValue
    var secondHighestTemp: Double = Int.MinValue
  }

  // 定义一个聚合函数，更新累加器内容
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    // 定义更新累加器内容的方法
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.highestTemp = temp
        acc.secondHighestTemp = acc.highestTemp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 定义结果输出方法
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }

}
