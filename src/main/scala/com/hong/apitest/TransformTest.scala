package com.hong.apitest

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStreamFromFile =
            env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        // 1. 基本转换算子
        val mapedStream: DataStream[SensorReading] = inputStreamFromFile
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })

        // todo
        //  DataStream先被keyBy() 转成 KeyedStream，
        //  然后使用KeyedStream的各种算子后，又转回DataStream

        // 2. 分组转换算子
        mapedStream
//            .keyBy(0)
            .keyBy("id")
//            .keyBy(new MyIdSelector)
//            .keyBy(data => data.id)
            .sum("temperature")

        mapedStream
            .keyBy("id")
            .min("temperature")
        // 只会打印最新的temperature，而key即id，还有时间戳，则是第一条信息的，不会变。
        // 并不是和最小temperature对应的timestamp，这个有点出乎意料。如果要对应，则需要使用 minBy()



        // 要打印 每个sensor最大timestamp，最小temperature
        mapedStream
            .keyBy("id")
//            .reduce(new MyReduce)     // 自定义的reduceFunction
            .reduce((curRes, newData) =>
                SensorReading(
                    curRes.id,
                    curRes.timestamp.max(newData.timestamp),
                    curRes.temperature.min(newData.temperature)
                )
            )


        // 3. 分流转换算子。 ①split ②select 成对使用
        // DataStream => SplitStream。
        // 需求：拆分为高温流和低温流
        val splitStream = mapedStream
            .split(data => {
                if(data.temperature > 30){
                    Seq("high")
                }else{
                    Seq("low")
                }
            })

        val highTempStream: DataStream[SensorReading] = splitStream.select("high")
        val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
        val allTempStream: DataStream[SensorReading] = splitStream.select("high","low")


//        highTempStream.print("high")
//        lowTempStream.print("low")
//        allTempStream.print("all")

        // 4. 合流，只能合两条流，而数据结构可以不一致。 Connect+CoMap 。 DataStream => CollectedStream => DataStream
        // 连接 温度传感器 和 温度传感器的两个不同传感器的数据
        val warningStream: DataStream[(String, Double)] = highTempStream.map(
            data => (data.id, data.temperature)
        )

        val connectedStream: ConnectedStreams[(String, Double), SensorReading] =
            warningStream.connect(lowTempStream)

        val resultStream: DataStream[Object] =
            connectedStream.map(
                warningData => (warningData._1, warningData._2, "hign temp warning"),
                lowTempData => (lowTempData.id, "normal")
                )

        // 5. union 合流，可以连接多条流，但数据结构需要一致。且至始至终都是 DataStream
        val unionStream : DataStream[SensorReading] =
            highTempStream.union(lowTempStream, allTempStream)

        env.execute()

    }

}

// 自定义函数类，key选择器
class MyIdSelector() extends KeySelector[SensorReading,String] {
    override def getKey(value: SensorReading): String = value.id
}

// 自定义reduce
class MyReduce() extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(
            value1.id,
            value1.timestamp.max(value2.timestamp),
            value1.temperature.min(value2.temperature)
        )
    }
}

// 自定义MapFunction
class MyMapper3 extends MapFunction[SensorReading, (String, Double)] {
    override def map(value: SensorReading): (String, Double) =
        (value.id, value.temperature)
}

// 自定义富函数：可以有生命周期方法，并可以获取运行时上下文，从而对运行过程中的State进行编程。
class MyRichMapper extends RichMapFunction[SensorReading, Int] {
    // 可以定义一些初始化的信息
    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def map(value: SensorReading): Int = value.timestamp.toInt
    //方法处理后的善后
    override def close(): Unit = super.close()
    // 获取上下文
    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext
}