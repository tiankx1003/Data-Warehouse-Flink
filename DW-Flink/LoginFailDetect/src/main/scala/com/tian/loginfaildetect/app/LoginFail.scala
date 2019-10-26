package com.tian.loginfaildetect.app

import java.util

import com.tian.loginfaildetect.bean.{LoginEvent, Warning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/26 10:53
 * @version 1.0.0
 */
object LoginFail {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val loginEventData: DataStream[LoginEvent] = env.readTextFile("files/LoginLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
            override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
        })
        val resultData: DataStream[Warning] = loginEventData
            .keyBy(_.userId)
            .process(new LoginFailWarning(5)) //
        resultData.print()
        env.execute("login fail job")
    }

    class LoginFailWarning(failTime: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
        //定义一个List状态，用于保存连续登录失败的案件
        private val loginFailListState: ListState[LoginEvent] =
            getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("logEvent-state", classOf[LoginEvent]))

        override def processElement(value: LoginEvent,
                                    ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                                    out: Collector[Warning]): Unit = {
            //判断是否失败时间，如果是，添加到状态，注册一个定时器
            if (value.status == "fail") {
                loginFailListState.add(value)
                ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() * 1000L + 2000L)
            } else {
                loginFailListState.clear()
            }
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
                             out: Collector[Warning]): Unit = {
            //判断状态列表中登录失败的个数
            import scala.collection.JavaConversions._
            val times: Int = loginFailListState.get().size
            if (times >= failTime)
                out.collect(Warning(
                    ctx.getCurrentKey,
                    loginFailListState.get().head.eventTime,
                    loginFailListState.get().last.eventTime,
                    "login fail"
                ))
        }

        //状态变成改进
        class LoginFailWarningAdv(failTime: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
            private val loginFailListState: ListState[LoginEvent] =
                getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("logEvent-state", classOf[LoginEvent]))

            override def processElement(value: LoginEvent,
                                        ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                                        out: Collector[Warning]): Unit = {
                if (value.status == "fail") { //按照status筛选出失败的时间，如果成功状态清空
                    //定义迭代器获取状态
                    val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
                    if (iter.hasNext) { //如果已经有失败事件才做处理，没有就直接add
                        val firstFailEvent: LoginEvent = iter.next()
                        //如果两次登录失败事件间隔小于2s，输出报警信息
                        if ((value.eventTime - firstFailEvent.eventTime).abs < 2) {
                            out.collect(
                                Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "login fail in 2s")
                            )
                        }
                        loginFailListState.clear()
                        loginFailListState.add(value) // TODO: 逻辑还有有问题
                    } else {
                        loginFailListState.add(value)
                    }
                } else {
                    loginFailListState.clear()
                }
            }
        }

    }

}
