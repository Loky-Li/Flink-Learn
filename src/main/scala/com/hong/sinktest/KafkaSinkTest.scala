package com.hong.sinktest

import com.hong.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
                    .toString
            })


//        dataStream.writeToSocket()
//        dataStream.addSink(new StreamingFileSink[SensorReading]())

        dataStream.addSink(new FlinkKafkaProducer011[String](
            "hadoop203:9092"
            ,"sinkTest"
            ,new SimpleStringSchema()

        ))

        env.execute("sink test")
    }

}
