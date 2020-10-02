package com.hong.tableapi

import com.hong.apitest.SensorReading
import com.sun.jmx.snmp.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object T7_WindowTest {
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

        // 1 窗口操作
        // 1.1 group 窗口，开一个10秒的滚动窗口，统计每个传感器温度的数量
        val groupResultTable: Table = sensorTable
            .window(Tumble over 10.seconds on 'ts as 'tw)
            .groupBy('id, 'tw)
            .select('id, 'id.count, 'tw.end)

        println("-------")

        // 1.2 Group 窗口SQL实现
        // todo 由于sensorTable只是从流得到的表，还没有在tableEnv注册
        tableEnv.createTemporaryView("sensor", sensorTable)
        val groupResultSqlTable: Table = tableEnv.sqlQuery(
            """
              |select
              | id,
              | count(id),
              | tumble_end(ts, interval '10' second)
              |from sensor
              |group by id,tumble(ts, interval '10' second)
              |""".stripMargin)


        // 2 Over Windows，
        // 2.1 对每个传感器统计每一行数据与前两行数据的平均温度
        val overResultTable: Table = sensorTable
            .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
            .select('id, 'ts, 'id.count over 'w, 'temperature.avg over 'w)

        // 2.2 Over窗口SQL实现。
        // （todo 相比PPT窗口只在一列使用，下面的同个窗口在两列中使用，写的方式可以简化如下）
        val overResultSqlTable: Table = tableEnv.sqlQuery(
            """
              |select id, ts,
              |    count(id) over w,
              |    avg(temperature) over w
              |from sensor
              |window w as (
              |     partition by id
              |     over by ts
              |     rows between 2 preceding and current row
              |)
              |""".stripMargin)

        // 由于上面的窗口，没有对迟到数据的处理，所以一次只有一个结果。直接使用append mod就够了。
        // 当然使用 toRetractStream 也可以。
        groupResultTable.toRetractStream[(String, Long, Timestamp)].print("group test")
        groupResultSqlTable.toAppendStream[Row].print("group sql test")

        overResultTable.toAppendStream[Row].print("over test")


        env.execute("time and window test")
    }

}
