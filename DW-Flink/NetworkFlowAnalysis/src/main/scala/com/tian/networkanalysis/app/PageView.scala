package com.tian.networkanalysis.app

import com.tian.networkanalysis.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 网站总浏览量统计
 *
 * @author tian
 * @date 2019/10/25 10:47
 * @version 1.0.0
 */
object PageView {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //引入事件时间
        env.setParallelism(8) //设置并行度
        val fileSourceData: DataStream[String] = env.readTextFile("files/UserBehavior.csv")
        val behaviorData: DataStream[UserBehavior] = fileSourceData.map(data => {
            val dataArr: Array[String] = data.split(",")
            //封装成样例类
            UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
        }).assignAscendingTimestamps(_.ts * 1000) //设置时间戳
        val resultData: DataStream[(String, Int)] = behaviorData
            .filter(_.behavior == "pv") //过滤出用户行为pv
            .map(_ => ("pv", 1))
            .keyBy(_._1)
            .timeWindow(Time.hours(1)) //滚动窗口长度1小时
            .sum(1)
        resultData.print
        env.execute()
    }
}
