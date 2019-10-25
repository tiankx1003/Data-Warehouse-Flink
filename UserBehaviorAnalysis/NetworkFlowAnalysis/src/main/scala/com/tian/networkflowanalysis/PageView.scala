package com.tian.networkflowanalysis

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * 网站总浏览量统计
 *
 * @author tian
 * @date 2019/10/25 10:48
 * @version 1.0.0
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
    def main(args: Array[String]): Unit = {
        val resourcesPath: URL = getClass.getResource("/UserBehaviorTest.csv")
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val stream: DataStreamSink[(String, Int)] = env.readTextFile(resourcesPath.getPath)
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv")
            .map(x => ("pv", 1))
            .keyBy(_._1)
            .timeWindow(Time.seconds(60 * 60))
            .sum(1)
            .print()

        env.execute("Page View Job")
    }
}

