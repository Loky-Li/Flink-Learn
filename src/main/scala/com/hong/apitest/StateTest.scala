package com.hong.apitest

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
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
    }
}

class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {

    lazy val myState: ValueState[Int] =
        getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state",classOf[Int]))

    // 另外一种定义 lazy state 的方式。由于程序运行的时候，才有 runtimeContext。所以可以放在生命周期中定义
    var myState2: ValueState[Int] = _
    override def open(parameters: Configuration): Unit = {
        myState2 =
            getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state2",classOf[Int]))
    }

    lazy val myListState: ListState[String] =
        getRuntimeContext.getListState(new ListStateDescriptor[String]("my-list-state",classOf[String]))

    override def processElement(
                                   value: SensorReading,
                                   ctx: KeyedProcessFunction[String, SensorReading, Int]#Context,
                                   out: Collector[Int]): Unit = {
        val value: Int = myState.value()
        myState.update(1)

        myListState.add("hello")
        myListState.addAll(new util.ArrayList[String]())

    }
}
