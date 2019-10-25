package com.tian.networkanalysis.app

import com.tian.networkanalysis.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 独立访客统计
 *
 * @author tian
 * @date 2019/10/25 19:47
 * @version 1.0.0
 */
object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(8)
        val fileSourceData: DataStream[String] = env.readTextFile("files/UserBehavior.csv")
        val behaviorData: DataStream[UserBehavior] = fileSourceData
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
            })
        val resultData: DataStream[UvCount] = behaviorData
            .filter(_.behavior == "pv")
            .timeWindowAll(Time.hours(1))
            .apply(new UvCountByAllWindow())
        resultData.print()
        env.execute()
    }
}

/**
 * 自定义all window function
 */
class UvCountByAllWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        var idSet: Set[Long] = Set[Long]() //使用Set做去重
        for (elem <- input)
            idSet += elem.userId //把每个数据放入set
        out.collect(UvCount(window.getEnd, idSet.size))
    }
}