package com.tian.marketanalysis

import java.util.Date
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/24 15:29
 * @version 1.0.0
 */
object AppMarketingStatistics {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource)
            .assignAscendingTimestamps(_.timestamp)

        stream
            .filter(_.behavior != "UNINSTALL")
            .map(data => {
                ("dummyKey", 1L)
            })
            .keyBy(_._1)
            .timeWindow(Time.hours(1), Time.seconds(1))
            .process(new MarketingCountTotal())
            .print()

        env.execute(getClass.getSimpleName)
    }
}

class MarketingCountTotal() extends ProcessWindowFunction[(String, Long), MarketingViewCount, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[MarketingViewCount]): Unit = {
        val startTs: Long = context.window.getStart
        val endTs: Long = context.window.getEnd
        val count: Int = elements.size
        out.collect(MarketingViewCount(formatTs(startTs).toLong, formatTs(endTs).toLong, "total", "total", count))
    }

    private def formatTs(ts: Long): String = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
        df.format(new Date(ts))
    }
}
