package com.tian.networkanalysis.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import com.tian.networkanalysis.bean.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
 * 实时流量统计
 *
 * @author tian
 * @date 2019/10/25 8:49
 * @version 1.0.0
 */
object NetworkFlow {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(8)
        val fileData: DataStream[String] = env.readTextFile("files/apache.log")
        val logData: DataStream[ApacheLogEvent] = fileData.map(line => {
            val lineArr: Array[String] = line.split(" ")
            val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val ts: Long = simpleDateFormat.parse(lineArr(3)).getTime
            ApacheLogEvent(lineArr(0), lineArr(2), ts, lineArr(5), lineArr(6))
        })
        val resultData: DataStream[String] = logData
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
                    override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
                })
            .filter(data => { // 过滤掉不重要的链接
                val pattern: Regex = "^((?!\\.(css|js)$).)*$".r
                (pattern findFirstIn data.url).nonEmpty
            })
            .keyBy(_.url)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .aggregate(new CountAgg(), new WindowResultFunction())
            .keyBy(_.windowEnd)
            .process(new TopHotUrls(5))
        resultData.print
        env.execute()
    }

    class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
        override def apply(key: String,
                           window: TimeWindow,
                           input: Iterable[Long],
                           out: Collector[UrlViewCount]): Unit = {
            out.collect(UrlViewCount(key, window.getEnd, input.iterator.next))
        }
    }

    class TopHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
        //private var urlState: ListState[UrlViewCount] = _
        // TODO: 使用MapState替换ListState
        private var urlState: MapState[String, Long] = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            val urlStateDesc: MapStateDescriptor[String, Long] =
                new MapStateDescriptor[String, Long]("urlState-state", classOf[String], classOf[Long])
            urlState = getRuntimeContext.getMapState(urlStateDesc)
        }

        override def processElement(i: UrlViewCount,
                                    context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                                    collector: Collector[String]): Unit = {
            //每条数据都保存到状态中
            urlState.put(i.url, i.count)
            //设置定时器
            context.timerService.registerEventTimeTimer(i.windowEnd + 1)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            //获取收到的所有Url访问量
            val allUrlViews: ListBuffer[(String, Long)] = ListBuffer()
            //            import scala.collection.JavaConversions._
            //            for (urlView <- urlState.get)
            //                allUrlViews += urlView
            //            urlState.clear()
            val iter: util.Iterator[util.Map.Entry[String, Long]] = urlState.entries().iterator()
            while (iter.hasNext) {
                val entry: util.Map.Entry[String, Long] = iter.next()
                allUrlViews += ((entry.getKey, entry.getValue))
            }
            val sortedUrlViews: ListBuffer[(String, Long)] =
                allUrlViews.sortBy(_._2)(Ordering.Long.reverse).take(topSize)
            //allUrlViews.sortWith(_.count > _.count).take(topSize)

            val result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ")
                .append(new Timestamp(timestamp - 1))
                .append("\n")
            for (elem <- sortedUrlViews.indices) {
                val currentUrlView: (String, Long) = sortedUrlViews(elem)
                result.append("No")
                    .append(elem + 1)
                    .append(":")
                    .append(" URL=")
                    .append(currentUrlView._1)
                    .append(" 流量=")
                    .append(currentUrlView._2)
                    .append("\n")
            }
            result.append("====================================\n\n")
            Thread.sleep(1000)
            out.collect(result.toString)
        }
    }

}
