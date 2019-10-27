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


//git
/**
 * @author tian
 * @date 2019/10/26 8:56
 * @version 1.0.0
 */
object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val sourceData: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource)
            .assignAscendingTimestamps(_.ts)
        val resultData: DataStream[MarketingViewCount] = sourceData
            .filter(_.behavior != "UNINSTALL")
            .map(data => ((data.channel, data.behavior), 1L))
            .keyBy(_._1)
            .timeWindow(Time.hours(1), Time.seconds(10))
            .process(new MarketingCountByChannel)
        resultData.print()
        env.execute("app marketing by channel job")
    }
    class MarketingCountByChannel() extends
        ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
        override def process(key: (String, String),
                             context: Context,
                             elements: Iterable[((String, String), Long)],
                             out: Collector[MarketingViewCount]): Unit = {
            val startTs: Timestamp = new Timestamp(context.window.getStart)
            val channel: String = key._1
            val behavior: String = key._2
            val count: Int = elements.size
            out.collect(MarketingViewCount(startTs.toString, channel, behavior, count))
        }
    }

}

