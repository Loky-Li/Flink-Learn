package com.hong.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object T4_KafkaTableTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val settings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        // 从kafka读取数据
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

        val sensorTable: Table = tableEnv
            .from("kafkaInputTable")

        // 转换输入表，得到结果表
        val resultTable: Table = sensorTable
            .select('id, 'temperature)
            .filter('id === "sensor_1")

        val aggResultTable: Table = sensorTable
            .groupBy('id)
            .select('id, 'id.count as 'count)

        // 定义一个连接到kafka的输出表
        tableEnv
            .connect(new Kafka()
                .version("0.11")
                .topic("sinkTest")
                .property("bootstrap.servers","hadoop203:9092")
                .property("zookeeper.connect","hadoop203.2181")
            )
            .withFormat(new Csv())
            .withSchema(
                new Schema()
                    .field("id",DataTypes.STRING())
                    .field("temp", DataTypes.DOUBLE())
//                        .field("cnt", DataTypes.BIGINT())
            )
            .createTemporaryTable("kafkaOutputTable" )

        // 将结果表输出
        resultTable.insertInto("kafkaOutputTable")

        env.execute("sink_kafka_test")
    }

}
