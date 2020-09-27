package com.hong.apitest


import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object CheckPointTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // todo checkpoint 相关配置。
        // 启用检查点，指定触发检查点间隔时间
        env.enableCheckpointing(1000L)

        // todo 其他配置
        //设置checkpoint的模式，精准一次等。
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        //每个checkpoint的超时时间
        env.getCheckpointConfig.setCheckpointTimeout(30000L)
        // 设置同一时间内，整个job最多允许做checkpoint的个数。（因为有些算子处理比较快）
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        // 当一个ck做了很长的时间，超过了第二个ck产生的时间点。
        // 设置了下面参数后，会在第二个ck该产生的时间的基础上，再等500L。（无论第一个ck是否做完？）
        // （如果第一个ck没有超过第二个ck产生时间，第二个ck正常产生）
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
        //  Sets whether a job recovery should fallback to checkpoint when there is a more recent savepoint.
        // 当存在savepoint的时候，是否优先从checkpoint中恢复数据。默认是false，即不优先从ck恢复。
        env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
        // 运行checkpoint 失败的次数
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

        // todo 重启策略的配置。
        // 常见方式1. 失败后，尝试重启3次，每次间隔 10秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3,
            10000L
        ))
        // 常见方式2. 在 5 分钟内，最多重启5次，每次重启间隔 5 秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
            5,
            Time.of(5,TimeUnit.MINUTES),
            Time.of(5, TimeUnit.SECONDS)
        ))

        val inputStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)

        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })

    }

}
