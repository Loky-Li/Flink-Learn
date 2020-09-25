package com.hong.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import com.hong.apitest.MySensorSource
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.hong.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

object WindowTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 设置事件时间

        val inputDataStream =
            env.addSource(new MySensorSource())
                //从数据中提取，事件时间依据的时间字段。这表示数据一定是标准升序
//            .assignAscendingTimestamps(_.timestamp*1000L)
                // 机制1：给乱序的延迟设置为2s。则WaterMark=currentMaxTimeStamp – 2s
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000L
            })



        val resultStream = inputDataStream
            .keyBy("id")
            //            .window(EventTimeSessionWindows.withGap(Time.minutes(1)))   // 会话窗口
            //            .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(-8)))   // 滚动窗口，东八区
            //            .window(SlidingProcessingTimeWindows.of(Time.days(1),Time.hours(-8)))   // 滑动窗口
            //            .timeWindow(Time.minutes(15),Time.minutes(5))     // 滑动时间窗口 。 底层调用的为上面的window()

            .timeWindow(Time.seconds(10))
            //            .reduce(new MyReduce())               // 测试1：增量聚合窗口

            // 机制2：除了水印保证乱序数据，还有运行迟到数据的机制
            // 先计算一次水位线前的窗口，然后允许1分钟后的迟到数据进来，再次计算，并关闭窗口。来一条迟到就计算并输出一次
            .allowedLateness(Time.minutes(1))
            //

            .apply(new MyWindowFunction()) // 测试2：全窗口函数

        inputDataStream.print("data")
        resultStream.print("result")

        env.execute()


    }

}

// 自定义一个全窗口函数
class MyWindowFunction() extends WindowFunction[SensorReading, Int, Tuple, TimeWindow] {
    override def apply(
                          key: Tuple, // keyBy() 后，从源码知，得到的Key是JavaTuple
                          window: TimeWindow,
                          input: Iterable[SensorReading],
                          out: Collector[Int]): Unit = {

        out.collect(input.toList.size) // 定义输出的是窗口元素量
    }
}

