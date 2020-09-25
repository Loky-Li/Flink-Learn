package com.hong.sinktest

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.scala._


object RedisSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)

            })
/*
        // 定义一个redis的配置类
        val conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop203")
                .setPort(6379)
                .build()
        // 定义一个RedisMapper
        val myMapper = new RedisMapper[SensorReading]{

        }
        dataStream.addSink(new RedisSink[SensorReading]())*/

        env.execute("sink test")
    }

}
