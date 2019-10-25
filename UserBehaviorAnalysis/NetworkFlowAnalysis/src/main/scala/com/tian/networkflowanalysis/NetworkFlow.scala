package com.tian.networkflowanalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实时流量统计
 * 
 * @author tian
 * @date 2019/10/24 11:42
 * @version 1.0.0
 */
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow{

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream: DataStreamSink[String] = env
            // 以window下为例，需替换成自己的路径
            .readTextFile("YOUR_PATH\\resources\\apache.log")
            .map(line => {
                val linearray: Array[String] = line.split(" ")
                val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val timestamp: Long = simpleDateFormat.parse(linearray(3)).getTime
                ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent]
                    (Time.milliseconds(1000)) {
                override def extractTimestamp(t: ApacheLogEvent): Long = {
                    t.eventTime
                }
            })
            .filter( data => {
                val pattern = "^((?!\\.(css|js)$).)*$".r
                (pattern findFirstIn data.url).nonEmpty
            } )
            .keyBy("url")
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .aggregate(new CountAgg(), new WindowResultFunction())
            .keyBy(1)
            .process(new TopNHotUrls(5))
            .print()
        env.execute("Network Flow Job")
    }

    class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
        override def createAccumulator(): Long = 0L
        override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1
        override def getResult(acc: Long): Long = acc
        override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
    }

    class WindowResultFunction extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {
        override def apply(key: Tuple,
                           window: TimeWindow,
                           aggregateResult: Iterable[Long],
                           collector: Collector[UrlViewCount]) : Unit = {
            val url: String = key.asInstanceOf[Tuple1[String]]._1 //.f0
            val count: Long = aggregateResult.iterator.next
            collector.collect(UrlViewCount(url, window.getEnd, count))
        }
    }

    class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {
        private var urlState : ListState[UrlViewCount] = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            val urlStateDesc: ListStateDescriptor[UrlViewCount] = new ListStateDescriptor[UrlViewCount]("urlState-state", classOf[UrlViewCount])
            urlState = getRuntimeContext.getListState(urlStateDesc)
        }

        override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
            // 每条数据都保存到状态中
            urlState.add(input)
            context.timerService.registerEventTimeTimer(input.windowEnd + 1)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 获取收到的所有URL访问量
            val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
            import scala.collection.JavaConversions._
            for (urlView <- urlState.get) {
                allUrlViews += urlView
            }
            // 提前清除状态中的数据，释放空间
            urlState.clear()
            // 按照访问量从大到小排序
            val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse)
                .take(topSize)
            // 将排名信息格式化成 String, 便于打印
            var result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

            for (i <- sortedUrlViews.indices) {
                val currentUrlView: UrlViewCount = sortedUrlViews(i)
                // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
                result.append("No").append(i+1).append(":")
                    .append("  URL=").append(currentUrlView.url)
                    .append("  流量=").append(currentUrlView.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000)
            out.collect(result.toString)
        }
    }
}

