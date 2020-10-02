package com.hong.tableapi

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object T3_OutputTableTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据创建DataStream
        val inputStream: DataStream[String] =
            env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })

        val settings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        val sensorTable: Table = tableEnv
            .fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

        // 转换输入表，得到结果表
        val resultTable: Table = sensorTable
            .select('id, 'temp)
            .filter('id === "sensor_1")

        val aggResultTable: Table = sensorTable
            .groupBy('id)
            .select('id, 'id.count as 'count)

        // todo 定义一张输出表，就是要写入数据的 TableSink
        tableEnv.connect(new FileSystem().path("E:\\code\\Flink-WSR\\src\\main\\resources\\out.txt"))
                .withFormat(new Csv())
                .withSchema(
                    new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
//                        .field("cnt", DataTypes.BIGINT())

                )
                .createTemporaryTable("outputTable")

        // 将结果表写入 table sink
        resultTable.insertInto("outputTable")
//        aggResultTable.insertInto("outputTable")    // 报错，同样的有记录的更改。insertInto只支持追加




//        sensorTable.printSchema()
        sensorTable.toAppendStream[(String, Double, Long)].print()

        env.execute("outputTableTest")
    }

}
