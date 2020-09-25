package com.hong.wc

import org.apache.flink.api.scala._

object WordCountBatch {
    def main(args: Array[String]): Unit = {
        // 创建一个批处理的执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        // 从文件中获取到
        val inputDataSet: DataSet[String] = env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\word.txt")

        // 基于DataSet做转换，首先按空格分词打散，然后按照word作为key做groupby
        val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet
            .flatMap(_.split(" "))
            .map((_, 1))
            .groupBy(0) // 以二元组中的第一个元素作为key分组
            .sum(1)     // 同上，以二元组的第二个元素的值进行聚合sum

        resultDataSet.print()

    }

}
