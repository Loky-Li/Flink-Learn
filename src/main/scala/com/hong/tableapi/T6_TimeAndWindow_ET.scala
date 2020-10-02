package com.hong.tableapi

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object T6_TimeAndWindow_ET {

    // todo 有三个地方和 PT 的不同：①env设置语义 ②分配时间戳依据 ③给定et字段。前两个是要多进行的操作
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // ①需要设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream: DataStream[String] =
            env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })
            // ② 分配时间戳
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
                'id, 'timestamp as 'ts, 'temperature,
                'et.rowtime)        // ③ et字段（方式一，定义新列）

      /*  val sensorTable: Table =
            tableEnv.fromDataStream(
                dataStream,
                'id, 'timestamp.rowtime as 'ts, 'temperature) // ③ et字段（方式一，在已有数据中指定）*/

        sensorTable.printSchema()

        sensorTable.toAppendStream[Row].print()

        env.execute("time and window test")
    }

}
