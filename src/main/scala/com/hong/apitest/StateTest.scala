package com.hong.apitest

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        /*    // 设置状态后端
            env.setStateBackend(new MemoryStateBackend())
            env.setStateBackend(new FsStateBackend(""))
            // RocksDB，需要引入依赖
            env.setStateBackend(new RocksDBStateBackend("",true))*/

        val inputStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)


        val dataStream: DataStream[SensorReading] = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })

        val resultStream = dataStream
            .keyBy("id")        // keyStream() extend DataStream, 所以keyBy() 后调用DataStream的 flatMap()方法，没有歧义。

            //            .flatMap(new TempChangeWraning2(10.0))        // 方式1：自定义富函数实现。 DataStream的方法。见上面解释。

            // 方式二：除了传入一个带状态编程的富函数，还可以直接使用算子。底层包装了状态变量的转换过程。
            // flatMapWithState[输出的类型,状态变量的泛型的数据类型]
            .flatMapWithState[(String, Double, Double), Double]({       // 只有keyStream() 有这个方法
                case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
                case (inputData: SensorReading, lastTemp: Some[Double]) => {
                    val curTemp: Double = inputData.temperature
                    val diff = (curTemp - lastTemp).abs
                    if (diff > 10.0) {
                        (
                            List((inputData.id, lastTemp, curTemp))
                            ,Some(curTemp)      // 更新状态变量
                        )

                    } else {
                        (
                            List.empty
                            ,Some(curTemp)
                        )
                    }
                }
            }

            )

        resultStream.print()

        env.execute("test-state")
    }
}

class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {


    // 使用 lazy 的原因是：程序一开始，并还没有 RuntimeContext！
    // scala知识：lazy 只能更 val 修饰的变量，但是可以更新
    lazy val myState: ValueState[Int] =
        getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))

    //todo 注意：new ValueStateDescriptor[Int]("my-state", classOf[Int])中 name+类型
    // 可以确定一个状态变量。所以如果 name + 类型 相同，即使状态变量 名称不同，在内存中都是同一份值
    // 所以下面 myState_other 和 myState， 是同一个状态变量。
    lazy val myState_other: ValueState[Int] =
        getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))

    // 另外一种定义 lazy state 的方式。由于程序运行的时候，才有 runtimeContext。所以可以放在生命周期中定义
    var myState2: ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
        myState2 =
            getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state2", classOf[Int]))
    }

    lazy val myListState: ListState[String] =
        getRuntimeContext.getListState(new ListStateDescriptor[String]("my-list-state", classOf[String]))

    lazy val myMapState: MapState[String, Double] =
        getRuntimeContext.getMapState(
            new MapStateDescriptor[String, Double](
                "my-map-state", classOf[String], classOf[Double]))

    // todo ReducingState 和 AggregateState
    lazy val myReducingState: ReducingState[SensorReading] =
        getRuntimeContext.getReducingState(
            new ReducingStateDescriptor[SensorReading](
                "my-reducing-state",
                new ReduceFunction[SensorReading] {
                    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
                        SensorReading(
                            value1.id,
                            value1.timestamp.max(value2.timestamp),
                            value1.temperature.min(value2.temperature)
                        )
                    }
                },
                classOf[SensorReading]
            )
        )


    ////================================================================
    override def processElement(
                                   value: SensorReading,
                                   ctx: KeyedProcessFunction[String, SensorReading, Int]#Context,
                                   out: Collector[Int]): Unit = {
        val date: Int = myState.value()
        myState.update(1)

        myListState.add("hello")
        myListState.addAll(new util.ArrayList[String]())

        myMapState.get("sensor_1")
        myMapState.put("sensor_2", 32.3)

        myReducingState.add(value)
    }
}

// 如果想实现一个算子状态的保存 （拓展，用得比较少。案例见底部！）
class MyReduceFunctionWithState extends ReduceFunction[SensorReading] with ListCheckpointed[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = ???

    // 将算子的状态怎么保存
    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[SensorReading] = ???

    // 发生故障后，怎么恢复数据
    override def restoreState(state: util.List[SensorReading]): Unit = ???
}


// 状态编程案例：两次温度值间的对比。
//可以使用ProcessFunction，但是ProcessFunction主要是为了 获取时间戳，获取水位线和注册定时器，
// 等等这样更加精细的场景中。该案例只需要使用使用富函数即可。只要获取到 runtimeContext 即可获取状态值。
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

    // 定义状态变量，上一次的温度。（也可以使用lazy的方式）
    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit =
        lastTempState = getRuntimeContext.getState(
            new ValueStateDescriptor[Double]("last-temp", classOf[Double]))


    override def map(value: SensorReading): (String, Double, Double) = {

        val lastTemp: Double = lastTempState.value()

        val diff: Double = (value.temperature - lastTemp).abs

        lastTempState.update(value.temperature)

        if (diff > threshold) {
            (value.id, lastTemp, value.temperature)
        } else
            (value.id, 0.0, 0.0)
        // 给null的话，如果没有达到报警条件，返回null。后面或空指针。
        // 因为map只能输出同类型的数据，且由于是 one-to-one，所以每条数据必须有输出。
        // todo 为了解决这个问题，使用flatMap，通过out.collect 来控制哪些需要输出
    }
}

// 自定义flateMapxxx
class TempChangeWraning2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    lazy val lastTempState: ValueState[Double] =
        getRuntimeContext.getState(new ValueStateDescriptor[Double](
            "last-temp2",
            classOf[Double]
        ))

    override def flatMap(value: SensorReading,
                         out: Collector[(String, Double, Double)]): Unit = {
        val lastTemp: Double = lastTempState.value()

        val curTemp: Double = value.temperature

        lastTempState.update(curTemp)

        val diff: Double = (curTemp - lastTemp).abs

        if (diff > threshold) {
            out.collect((value.id, lastTemp, curTemp))
        } else {
            null
        }
    }
}

// operator state 示例：在keyBy() 前
// 需求：统计所有输入的数据条数
class MyMapper() extends RichMapFunction[SensorReading,Long] {
/*
    operator state 没有 valueState这个数据类型，它是 keyed state的数据类型
    lazy val countState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-records",classOf[Long]))
        */

    var count: Long = 0L

    override def map(value: SensorReading): Long = {
        count += 1
        count
        // 如果发生故障，这个值就丢了，因为是jvm的变量。（是否还有不同分区的count不同的问题呢？）

    }
}

// todo 优化 将 operator state 的变量状态化保存
class MyMapper2() extends RichMapFunction[SensorReading,Long] with ListCheckpointed[Long]{

    var count: Long = 0L

    override def map(value: SensorReading): Long = {
        count += 1
        count
    }

    // 将需要保存的值，做快照保存到List中
    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
        val stateList = new util.ArrayList[Long]()
        stateList.add(count)
        stateList
    }

    // 将保存的状态，恢复出来。（如发生故障，将保存的状态取出）
    override def restoreState(state: util.List[Long]): Unit = {
        val iter: util.Iterator[Long] = state.iterator()
        while(iter.hasNext){
            count += iter.next()
        }

//        for(countState <- state){
//            count += countState
//        }


    }
}

