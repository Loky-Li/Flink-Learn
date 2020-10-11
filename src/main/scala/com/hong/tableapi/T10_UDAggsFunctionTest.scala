package com.hong.tableapi

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object T9_TableFunctionTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream: DataStream[String] =
            env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
                    override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
                })

        val settings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        // 将DataStream转Table
        val sensorTable: Table =
            tableEnv.fromDataStream(
                dataStream,
                'id, 'timestamp.rowtime as 'ts, 'temperature
            )

        // 创建一个UDF的实例
        val split = new Split("_")
        // 调用Table API。
        // 相比于标量函数输出的是一个值，TableFunction输出的是一个表
        // 所以TableFunction使用的时候，需要调用joinLateral方法。
        val resultTable = sensorTable
                .joinLateral(split('id) as ('word, 'length))
                .select('id, 'ts, 'word, 'length)

        // SQL实现
        tableEnv.createTemporaryView("sensor", sensorTable)
        tableEnv.registerFunction("split", split)
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select id, ts, word, length
              |from
              |sensor, lateral table( split(id) ) as split_id(word, length)
              |""".stripMargin)

        resultTable.toAppendStream[Row].print("result")
        resultSqlTable.toAppendStream[Row].print("sql result")

        env.execute("table function test")
    }

}

// 自定义 一个 Table Function，对一个 String ，输出用某个分隔符切分后的（word,wordLength）
class Split(separator: String) extends TableFunction[(String, Int)]{
    def eval(str: String): Unit = {
        str.split(separator).foreach(
            word => collect((word, word.length))
        )
    }
}
