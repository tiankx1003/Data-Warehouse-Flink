package com.tian.marketanalysis

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 页面广告分析
 * 页面广告点击统计
 * 黑名单过滤
 *
 * @author tian
 * @date 2019/10/24 15:33
 * @version 1.0.0
 */
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {

    val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blacklist")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val adLogStream: DataStream[AdClickLog] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val filterBlackListStream: DataStream[AdClickLog] = adLogStream
            .keyBy(logData => (logData.userId, logData.adId))
            .process(new FilterBlackListUser(100))

        val adCountStream: DataStreamSink[CountByProvince] = filterBlackListStream
            .keyBy(_.province)
            .timeWindow(Time.minutes(60), Time.seconds(5))
            .aggregate(new countAgg(), new countResult())
            .print()

        filterBlackListStream.getSideOutput(blackListOutputTag)
            .print("black list")

        env.execute("ad statistics job")
    }

    class FilterBlackListUser(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
        // 保存当前用户对当前广告的点击量
        lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
        // 标记当前（用户，广告）作为key是否第一次发送到黑名单
        lazy val firstSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstsent-state", classOf[Boolean]))
        // 保存定时器触发的时间戳，届时清空重置状态
        lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

        override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
            val curCount: Long = countState.value()
            // 如果是第一次处理，注册一个定时器，每天 00：00 触发清除
            if (curCount == 0) {
                val ts: Long = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
                resetTime.update(ts)
                ctx.timerService().registerProcessingTimeTimer(ts)
            }
            // 如果计数已经超过上限，则加入黑名单，用侧输出流输出报警信息
            if (curCount > maxCount) {
                if (!firstSent.value()) {
                    firstSent.update(true)
                    ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
                }
                return
            }
            // 点击计数加1
            countState.update(curCount + 1)
            out.collect(value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
            if (timestamp == resetTime.value()) {
                firstSent.clear()
                countState.clear()
            }
        }
    }

}


class countAgg() extends AggregateFunction[AdClickLog, Long, Long] {
    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class countResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
        out.collect(CountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
    }

    private def formatTs(ts: Long): String = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
        df.format(new Date(ts))
    }
}


