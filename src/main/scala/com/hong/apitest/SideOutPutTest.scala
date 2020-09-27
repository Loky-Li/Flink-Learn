package com.hong.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPutTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)

        val dataStream = inputStream.map(
            string => {
                val strArr: Array[String] = string.split(",")
                SensorReading(strArr(0),strArr(1).toLong,strArr(2).toDouble)
            }
        )

        // 用ProcessFunction 的侧输出流实现分流操作
        val highTempStream = dataStream
            .process(new SplitTempProcessor(30.0))

        // 侧输出流的数据类型可以和主流不同
        val lowTempStream =
            highTempStream.getSideOutput(new OutputTag[(String,Double,Long)]("low-temp"))

        // 打印输出
        highTempStream.print("high")
        lowTempStream.print("low")

        env.execute("side output job")

    }
}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading,SensorReading] {
    override def processElement(
                                   value: SensorReading,
                                   ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                   out: Collector[SensorReading]): Unit = {
        // 判断当前数据的温度值和阈值的对比
        if(value.temperature > threshold){
            out.collect(value)      // 大于阈值，主流输出
        }else{
            ctx.output(new OutputTag[(String,Double,Long)]("low-temp"),(value.id,value.temperature,value.timestamp))            // 侧输出
        }
    }
}
