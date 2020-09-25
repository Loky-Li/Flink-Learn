package com.hong.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCountStream {
    def main(args: Array[String]): Unit = {

        // 创建流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//        env.disableOperatorChaining()     // 全局禁用任务链

        // 接收socket文本流
//        val inputDataStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)


        // 将上面优化，从程序的参数中读取hostname和port
        val paramTool: ParameterTool = ParameterTool.fromArgs(args)
        val hostname: String = paramTool.get("host")
        val port: Int = paramTool.getInt("port")

        val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

        // 定义转换操作
        val resultDataStream: DataStream[(String,Int)] = inputDataStream
            .flatMap(_.split(" ")).slotSharingGroup("1")
//            .flatMap(_.split(" ")).slotSharingGroup("1") 可以将算子放在不同的slot共享组中，
            // 没有指定，所有算子共享一个组。如果指定，如下面的filter放在“2”组的时候，
            // flatmap和filter就不能在同一个slot中
            .filter(_.nonEmpty)
            .map((_,1))
//            .map((_,1)).disableChaining()   map不和前面及后面的算子组成任务链。
            .keyBy(0)
          // keyBy() 非数据操作算子，无法设置并行度！
            .sum(1)
//            .sum(1).startNewChain()   开启新的任务链，将前面的断开。


        resultDataStream.print().setParallelism(1)
        // 流式处理的时候，需要启动
        env.execute("stream word count job")

    }

}
