package com.hong.tableapi

import com.sun.prism.PixelFormat.DataType
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}



object T2_TableApiTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //  todo 1. 创建表环境
        val tableEnv = StreamTableEnvironment.create(env)

        // 1.1 创建老版本的流查询环境
        val setting: EnvironmentSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv2 = StreamTableEnvironment.create(env,setting)

        // 1.2 创建老版本的批式查询环境
        val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

        // 1.3 创建blink版本的流查询环境
        val bsSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val bsTableEnv = StreamTableEnvironment.create(env,bsSettings)

        // 1.4 创建blink版本的批式查询环境
        val bbSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inBatchMode()
            .build()
//        val bbTableEnv = TableEnvironment.create(bbSettings)      // 需要在在scala compiler中配置 “-target:jvm-1.8”

        // todo 2. 从外部系统读取数据，在环境中注册表
        // 2.1 连接到文件系统
        val filePath = "E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt"
        tableEnv
            .connect(new FileSystem().path(filePath))
//            .withFormat(new OldCsv())           // 定义读取数据之后的格式化方法
            .withFormat(new Csv())              // 引入依赖，不使用以前过时的 OldCsv()
            .withSchema(
                new Schema()
                    .field("id",DataTypes.STRING())
                    .field("timestamp",DataTypes.BIGINT())
                    .field("temperature",DataTypes.DOUBLE())
            )                                   // 定义表结构
            .createTemporaryTable("inputTable" )    // 注册一张表


        // 2.2 连接到kafka
        tableEnv
            .connect(new Kafka()
                    .version("0.11")
                    .topic("sensor")
                    .property("bootstrap.servers","hadoop203:9092")
                    .property("zookeeper.connect","hadoop203.2181")
            )
            .withFormat(new Csv())
            .withSchema(
                new Schema()
                    .field("id",DataTypes.STRING())
                    .field("timestamp",DataTypes.BIGINT())
                    .field("temperature",DataTypes.DOUBLE())
            )
            .createTemporaryTable("kafkaInputTable" )

        // todo 3 表查询
        // 3.1 简单查询，过滤投影
        val sensorTable: Table = tableEnv.from("inputTable")
        val resultTable: Table = sensorTable
            .select('id, 'temperature)
            .filter('id === "sensor_1")
        // 3.2 SQL 简单查询
        val resultSqlTable: Table = tableEnv.sqlQuery(
            """
              |select id, temperature
              |from inputTable
              |where id = 'sensor_1'
              |""".stripMargin
        )

        //  3.3 简单聚合
        val aggResultTable: Table = sensorTable
            .groupBy('id)
            .select('id, 'id.count as 'count)
        // 3.4 SqL 实现简单聚合
        val aggResultSqlTable: Table = tableEnv.sqlQuery(
            """
              |select id, count(id) as cnt
              |from inputTable
              |group by id
              |""".stripMargin)


        // 转换成流打印输出

        val sensorTableStream: DataStream[(String, Long, Double)] =
            sensorTable.toAppendStream[(String, Long, Double)]      // 注意将import的 flink.table.api.scala_

        sensorTable.printSchema()
        sensorTableStream.print("table api test")

        println("==================")
        resultSqlTable.toAppendStream[(String, Double)].print("sql table")

        // 下面会报错：Table is not an append-only table. Use the toRetractStream()
        // in order to handle add and retract messages.
        // 原因：group by不是来一天，插入一条到表中，而是对已有的数据进行更新。如
        //来了一条sensor_1的数据，得到结果 （sensor_1,1），再来一条的时候，该输出会变更为 （sensor_1, 2）
//        aggResultSqlTable.toAppendStream[(String, Long)].print("agg")
        // todo 对于有更新、装换的结果，需要使用下面的方式
        aggResultSqlTable.toRetractStream[(String, Long)].print("agg")

        env.execute("table api test")


    }

}
