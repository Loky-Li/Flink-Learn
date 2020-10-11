package com.hong.apitest

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunction_CoPF {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //数据源
        val readings: DataStream[SensorReading] = env.addSource(new MySensorSource)

        //开关流：只要有一个数据，当数据进入时，上面的流被放行10s。只放10s
        val filterSwitches = env.fromCollection(Seq(
            ("sensor_2", 10 * 1000L)
            //这是一条控制readings流放行的数据种类及时间的指令信息。通过读取这条信息，交给方法处理从而实现控制。
        ))

        val forwardedReadings = readings
            .connect(filterSwitches)
            .keyBy(_.id,_._1)
            .process(new ReadingFilter)


        forwardedReadings.print()


        env.execute()
    }

    class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

        //传输数据的开关。默认 false
        lazy val forwardingEnable = getRuntimeContext.getState(
            new ValueStateDescriptor[Boolean]("filterSwitch",Types.of[Boolean])
        )

        //关闭开关的定时器的时间戳
        lazy val disableTimer = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("timer",Types.of[Long])
        )


        //第一条流的处理逻辑
        override def processElement1(value1: SensorReading,
                                     ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                     out: Collector[SensorReading]): Unit = {
            //如果开关是打开的状态，则将第一条流的数据写出
            if(forwardingEnable.value()){
                out.collect(value1)
            }
        }


        //第二条流的处理逻辑。只有一条数据，实现设置开关的状态
        override def processElement2(value2: (String, Long),
                                     ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                     out: Collector[SensorReading]): Unit = {

            //第二条流进入，则开关打开，开流
            forwardingEnable.update(true)

            //开流后，开始设置一个10s后的定时器。10s这个值是第二条流的第二个值
            //先获取需要注册的定时器的时间戳
            val timerTs: Long = ctx.timerService().currentProcessingTime() + value2._2

            //获取当前存在的定时器的时间戳（其实不存在，只有一条数据，无需判断。只是为了如果是其他场景时的通用性）
            val curTimerTs = disableTimer.value()
            //如果当前需要设置的定时器的时间戳大于已经存在的时间戳，才开始注册（注册前需要删除掉旧的定时器）
            if(timerTs > curTimerTs){
                //先删除旧的定时器
                ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
                //注册新的定时器
                //todo 下面我一句代码错注册成了事件时间的定时器，造成本例中的
                // 处理时间定时器根本没有注册成功，搞得我找了好久问题！
                // ctx.timerService().registerEventTimeTimer(timerTs)
                ctx.timerService().registerProcessingTimeTimer(timerTs)
                //更新保存的定时器的时间戳
                disableTimer.update(timerTs)
            }

        }

        override def onTimer(timestamp: Long,
                             ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                             out: Collector[SensorReading]): Unit = {
            //触发了定时器后，也就是第一条流不再输出。则forwardingEnable变成false（也是布尔值的默认值）
            //forwardingEnable在处理的过程中，变成了true，所以情况变量的状态就是默认的false，实现关流
            //            forwardingEnable.update(false)
            forwardingEnable.clear()

            //同时将定时器的状态清空
            disableTimer.clear()
        }
    }

}
