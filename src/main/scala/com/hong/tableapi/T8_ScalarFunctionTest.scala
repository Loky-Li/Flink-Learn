package com.hong.tableapi

import com.hong.apitest.SensorReading
import com.sun.jmx.snmp.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object T8_ScalarFunctionTest {
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
        val hashCode = new HashCode(10)
        val resultTable: Table =
            sensorTable.select('id, 'ts, hashCode('id))

        // SQL实现
        tableEnv.createTemporaryView("sensor", sensorTable)
        tableEnv.registerFunction("hashcode",hashCode)
        val resultSqlTable = tableEnv.sqlQuery(
            "select id, ts, hashcode(id) from sensor"
        )

        resultTable.toAppendStream[Row].print("result")
        resultSqlTable.toAppendStream[Row].print("sql result")

        env.execute("scalar function test")
    }
}

/// 自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
    // 必须实现一个eval方法，它的参数是当前传入的字段，它的输出是一个Int类型的hash值
    def eval(str: String): Int = {
        str.hashCode * factor
    }
}
