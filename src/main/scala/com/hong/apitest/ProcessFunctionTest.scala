package com.hong.apitest

import org.apache.flink.streaming.api.scala._
import com.hong.apitest._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object ProcessFunctionTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

        val inputStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)

        val dataStream = inputStream.map(
            string => {
                val strArr: Array[String] = string.split(",")
                SensorReading(strArr(0),strArr(1).toLong,strArr(2).toDouble)
            }
        )


        // 需求：检查每一个传感器10s内，温度是否连续上升。如符合，则报警
        val warningStream = dataStream
            .keyBy("id")
            .process(new TempIncreWarning(10000L))

        warningStream.print()
        env.execute("process function job")

    }

}

class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple,SensorReading,String] {

    // 由于需要跟之前的温度值做对比，所以将上一个温度保存成状态
    // 使用了ValueStareDescriptor后，状态变量时按照key隔离。 （不是很理解）
    lazy val lastTempState: ValueState[Double] =
        getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

    // 为了方便删除定时器，还需要保存定时器的时间戳（即触发定时任务的时间）
    lazy val curTimerTsState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts",classOf[Long]))

    var curTemp: Double = _


    // 每条数据进来的时候，调用一次processElement() 方法
    override def processElement(
                                   value: SensorReading,
                                   ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context,
                                   out: Collector[String]): Unit = {
//        ctx.timerService()    // 使用timeService，可以获取很多时间服务功能

        // 取出状态
        val lastTemp: Double = lastTempState.value()
        val curTimerTs: Long = curTimerTsState.value()

        // 更新温度状态：
        curTemp=value.temperature

        lastTempState.update(curTemp)

        // 更新定时器状态：如果比之前的温度高，且没有定时器，注册10秒后的定时器。
        // 状态变量的默认值是0。如果定时器没有注册过，即没有调用过update()方法，则为状态变量的默认值0
        // 所以，温度的默认值也是0，则只要输入一个大于0的温度值，即使只是第一条数据，也会触发。
        if(curTemp >= lastTemp && curTimerTs == 0){
            val curTs: Long = ctx.timerService().currentProcessingTime() + interval
            ctx.timerService().registerProcessingTimeTimer(curTs)
            curTimerTsState.update(curTs)
        }
        // 如果温度下降，删除定时器，并清空状态
        else if(curTemp < lastTemp){
            ctx.timerService().deleteProcessingTimeTimer(curTimerTs.toLong)
            // 清空状态变量 （注意，定时器，和定时器的状态变量时两个不同的实体）
            curTimerTsState.clear()

        }

    }

    // 定时器触发时，调用 onTimer() 方法。10秒内没有下降的温度，报警

    // 如果注册了多个定时器，由于只有一个onTimer方法。但是不同的定时器想不同的处理逻辑，
    // 则可以通过多个定时器的时间戳，和 onTimer的 timestamp进行对比判断。
    override def onTimer(
                            timestamp: Long,
                            ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext,
                            out: Collector[String]): Unit = {
        out.collect(ctx.getCurrentKey + "温度连续" + interval/1000 + "秒上升")
        curTimerTsState.clear()     // 清空定时器时间戳状态变量
        // 定时器触发的时候，重新监控，新一个轮回。也可以不清空，以该轮的温度为基准，继续监控。
        lastTempState.clear()
    }
}