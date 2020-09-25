package com.hong.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.functions.source._

import scala.util.Random

object SourceTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 1. 从集合中读取数据
        val stream1: DataStream[SensorReading] = env.fromCollection(
            List(
                SensorReading("sensor_1", 1547718199, 35.8),
                SensorReading("sensor_6", 1547718201, 15.4),
                SensorReading("sensor_7", 1547718202, 6.7),
                SensorReading("sensor_10", 1547718205, 38.1)
            )
        )

        // 2. 从文件中读取数据
        val stream2: DataStream[String] =
            env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        // 3. sockert 文本流

        // 4. kafka
        // 需要引入 flink 和 kafka之间的连接器
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop203:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        val stream4 =
            env.addSource(
                new FlinkKafkaConsumer011[String](
                    "sensor",
                    new SimpleStringSchema(),
                    properties
                ))

        // 5. 自定义source
        val stream5 = env.addSource(new MySensorSource())

        // 打印输出
        stream5.print("Stream1")
        env.execute("source test job")
    }

}

// 输入数据的样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)


// 自定义的SourceFunction， 使用场景：① 不知道数据的来源组件，只看到数据。 ② 自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading] {

    // 定义 flag ，表示数据源是否正常运行
    var running: Boolean = true

    // cancel：不再读取数据源数据
    override def cancel(): Unit = {
        running = false
    }


    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

        // 定义一个随机数生成器
        val rand = new Random()

        // 随机生成10个传感器的温度值，并且不停在之前温度的基础上更新（随机上下波动）
        // 首先生成10个传感器的初始温度
        var curTemps = 1.to(10).map(
            i => ("sensor_" + i, 60 + rand.nextGaussian()*20)
        )
        // 无限循环生成随机数据流
        while(running){
            // 在当前温度的基础上，随机生成微小波动
            curTemps = curTemps.map{        // curTemps 为10个传感器的温度元组，则一次产生10个传感器的随机温度
                case (sensor,temp) => (sensor, temp + rand.nextGaussian())
            }

            // 获取当前系统时间
            val curTs = System.currentTimeMillis()

            // 包装成样例类，用ctx发出数据
            curTemps.foreach{
                case (sensor, temp) => ctx.collect(SensorReading(sensor,curTs,temp))
            }

            // 定义间隔时间,1s
            Thread.sleep(1000)

        }
    }


}