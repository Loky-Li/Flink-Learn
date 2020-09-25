package com.hong.sinktest

import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

object EsSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })


//        dataStream.writeToSocket()
//        dataStream.addSink(new StreamingFileSink[SensorReading]())

//        dataStream.addSink(new ElasticsearchSink)

        env.execute("sink test")
    }

}
