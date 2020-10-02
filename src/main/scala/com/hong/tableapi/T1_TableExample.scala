package com.hong.tableapi

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

object T1_TableExample {
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

        // 创建表执行环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

        // 基于数据流，装换成一张表，然后进行操作
        val dataTable: Table = tableEnv.fromDataStream(dataStream)

        // 方式一：调用Table API，得到转换为表结构样式的结果
        val resultTable: Table = dataTable
            .select("id, temperature")
            .filter("id == 'sensor_1'")

        // 方式二： 使用熟悉的sql语句
/*        val resultTable_2: Table = tableEnv.sqlQuery(
            "select id, temperature from" + dataTable +
            "where id = 'sensor_1'")*/

        // 打印表的元数据
        resultTable.printSchema()
        // Table => DataStream[Row] 类型，打印
        val resultStream: DataStream[(String,Double)] =
            resultTable.toAppendStream[(String,Double)]
        resultStream.print("result")

        println("=======================")

        // 打印表的元数据
/*
        resultTable_2.printSchema()
        // Table => DataStream[Row] 类型，打印
        val resultStream_2: DataStream[(String,Double)] =
            resultTable_2.toAppendStream[(String,Double)]
        resultStream_2.print("result")
*/


        env.execute("table example job")
    }

}
