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
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark

object WindowTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 点setStreamTimeCharacteristic源码可知：
        // 当设置为EvenTime的时候，没200ms 生成一个WaterMark。
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 设置事件时间
        // 改变周期性自动生成WaterMark的时间周期
        env.getConfig.setAutoWatermarkInterval(500L)  // 修改为每 500毫秒生成一个WaterMark


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
            // 先计算一次水位线前的窗口，然后允许1分钟后的迟到数据进来，再次计算，并关闭窗口。
            // 来一条迟到就计算并输出一次
            .allowedLateness(Time.minutes(1))
            // 机制3：侧输出流。整个窗口关闭后，还有迟到数据。
            // 将原来已关闭的窗口聚合结果再拿出来和迟到数据计算
            .sideOutputLateData(new OutputTag[SensorReading]("late_test"))

            .apply(new MyWindowFunction()) // 测试2：全窗口函数

        inputDataStream.print("data")
        resultStream.print("result")

        // 打印迟到的侧输出流
        resultStream.getSideOutput(new OutputTag[SensorReading]("late_test"))

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

// 自定义一个周期性生成WaterMark的Assigner。 （模拟BoundedOutOfOrdernessTimestampExtractor）
// 看源码中：assignTimestampsAndWatermarks 的传入参数注释
class MyWMAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

    // 需要两个关键参数：延迟时间和当前数据的最大时间戳
    val lateness: Long = 1000L
    var maxTs: Long = Long.MinValue

    override def getCurrentWatermark: Watermark =
        new Watermark(maxTs - lateness)

    // 每来一条数据，就会调用一次extractTimestamp方法
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        maxTs = maxTs.max(element.timestamp * 1000L)
        element.timestamp * 1000L
    }
}

// 自定义一个断点式生成WaterMark的Assigner
class MyWMAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading] {
    val lateness: Long = 1000L

    // 每来一条数据的时候，两个方法都会调。先check，更新lastElement。后extract，提取时间戳
    override def checkAndGetNextWatermark(
                                             lastElement: SensorReading,
                                             extractedTimestamp: Long): Watermark = {
            // 只有 sensor_1 传感器的数据来的时候，才生成一个WaterMark
        if(lastElement.id == "sensor_1"){
            new Watermark(extractedTimestamp - lateness)
        }else
            null
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long =
        element.timestamp * 1000L
}