package com.tian.marketanalysis.app

import java.sql.Timestamp

import com.tian.marketanalysis.bean.{AdClickLog, BlackListWarning, CountByProvince}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/26 9:19
 * @version 1.0.0
 */
object AdStatisticByGeo {
    private val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blacklist")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(8)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val fileSourceData: DataStream[String] = env.readTextFile("files/AdClickLog.csv")
        val clickData: DataStream[AdClickLog] = fileSourceData.map(data => {
            val dataArr: Array[String] = data.split(",")
            AdClickLog(dataArr(0).toLong, dataArr(1).toLong, dataArr(2), dataArr(3), dataArr(4).toLong)
        })
            .assignAscendingTimestamps(_.ts * 1000L)
        val blackListData: DataStream[AdClickLog] = clickData
            .keyBy(data => (data.userId, data.adId))
            .process(new FilterBlackListUser(50)) //点击超过50次时加入黑名单进行过滤
        val countData: DataStream[CountByProvince] = blackListData
            .keyBy(_.province)
            .timeWindow(Time.hours(1), Time.seconds(10))
            .aggregate(new CountAgg, new AdCountResult)
        countData.print("count")
        blackListData.print("blackList")
        env.execute()
    }

    class CountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: AdClickLog, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String,
                           window: TimeWindow,
                           input: Iterable[Long],
                           out: Collector[CountByProvince]): Unit = {
            val ts: Timestamp = new Timestamp(window.getEnd)
            out.collect(CountByProvince(ts.toString, key, input.size)) // TODO: ???
        }
    }

    /**
     * 用于过滤点击次数超限的用户
     *
     * @param maxCount
     */
    class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
        // TODO: 懒加载
        lazy val countState: ValueState[Long] = //定义状态，保存用户对广告的点击量
            getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
        lazy val isFirstSent: ValueState[Boolean] = //标记是否发送过黑名单信息
            getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isFirst-state", classOf[Boolean]))
        lazy val resetTime: ValueState[Long] = //保存定时器触发的时间戳
            getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state", classOf[Long]))

        override def processElement(value: AdClickLog,
                                    ctx: KeyedProcessFunction[(Long, Long),
                                        AdClickLog, AdClickLog]#Context,
                                    out: Collector[AdClickLog]): Unit = {
            //获取当前count
            val curCount: Long = countState.value()
            if (curCount == 0) { //判断如果是第一条数据，count是0，就注册一个定时器
                val ts: Long = //下一天的零点对应时间戳的(毫秒数)
                    ctx.timerService().currentProcessingTime() / ((1000 * 60 * 60 * 24) + 1) * 24 * 60 * 60 * 1000
                ctx.timerService().registerProcessingTimeTimer(ts)
                resetTime.update(ts)
            }
            if (curCount >= maxCount) { //判断计数器是否超出上限，如果超过输出黑名单信息到侧输出流
                if (!isFirstSent.value()) {
                    isFirstSent.update(true)
                    ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
                }
            } else {
                out.collect(value) // TODO: 如果不进循环就不collect???
            }
            countState.update(curCount + 1)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[(Long, Long),
                                 AdClickLog, AdClickLog]#OnTimerContext,
                             out: Collector[AdClickLog]): Unit = {
            if (timestamp == resetTime.value()) {
                isFirstSent.clear()
                countState.clear()
            }
        }
    }

}

