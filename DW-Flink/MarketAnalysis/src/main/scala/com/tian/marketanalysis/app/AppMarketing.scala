package com.tian.marketanalysis.app

import java.sql.Timestamp

import com.tian.marketanalysis.bean.{MarketingUserBehavior, MarketingViewCount}
import com.tian.marketanalysis.source.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/26 9:07
 * @version 1.0.0
 */
object AppMarketing {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(8)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val sourceData: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource)
            .assignAscendingTimestamps(_.ts)
        val resultData: DataStream[MarketingViewCount] = sourceData
            .filter(_.behavior != "UNINSTALL")
            .map(data => ("dummyKey", 1L))
            .keyBy(_._1)
            .timeWindow(Time.hours(1), Time.seconds(10))
            .process(new MarketingCountTotal())
        resultData.print()
        env.execute()
    }
    class MarketingCountTotal() extends ProcessWindowFunction[(String, Long), MarketingViewCount, String, TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[(String, Long)],
                             out: Collector[MarketingViewCount]): Unit = {
            val startTs: Timestamp = new Timestamp(context.window.getStart)
            out.collect(MarketingViewCount(startTs.toString, "total channel", "total behavior", elements.size))
        }
    }
}

