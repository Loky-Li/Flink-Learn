package com.hong.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hong.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object JdbcSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.readTextFile("E:\\code\\Flink-WSR\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
            })


//        dataStream.writeToSocket()
//        dataStream.addSink(new StreamingFileSink[SensorReading]())

        dataStream.addSink(
            new MyJdbcSink()
        )

        env.execute("jdbc sink test")
    }
}

/*
// 自定义一个 SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{

    // 首先定义sql连接，以及预编译语句
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    // 在 open 生命周期方法中创建连接及预编译语句
    override def open(parameters: Configuration): Unit = {
        conn = DriverManager
            .getConnection(
                "jdbc:mysql://192.168.6.100:3306/test",
                "root",
                "root")

        insertStmt = conn.prepareStatement("insert into temp (sensor, temperature) (?,?)")
        updateStmt = conn.prepareStatement("update temp set temperature=? where sensor=?")
    }

    // 数据sink输出到外部组件
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        // 执行更新语句
        updateStmt.setDouble(1, value.temperature)
        updateStmt.setString(2, value.id)
        updateStmt.execute()

        // 如果没有更新语句，那么执行插入操作
        if(updateStmt.getUpdateCount == 0){
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temperature)
            insertStmt.execute()
        }
    }

    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }

}
*/

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    // open 主要是创建连接
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        conn = DriverManager.getConnection(
            "jdbc:mysql://192.168.6.100:3306/test",
            "root",
            "123456")
        insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
        updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
    }
    // 调用连接，执行sql
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {

        updateStmt.setDouble(1, value.temperature)
        updateStmt.setString(2, value.id)
        updateStmt.execute()

        if (updateStmt.getUpdateCount == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temperature)
            insertStmt.execute()
        }
    }

    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }
}

