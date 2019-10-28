package com.tian.ordertimeoutdetect.app

import com.tian.ordertimeoutdetect.bean.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/28 10:15
 * @version 1.0.0
 */
// TODO: 待补全
object OrderTimeoutWithoutCep {
    val orderTimeoutOutputTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // TODO: 考虑使用ProcessingTime语义
        // TODO: 尝试使用非周期性生成watermark
        val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile("files/OrderLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
            })
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.orderId)
        val timeoutData: DataStream[OrderResult] = //orderEventStream.process(new OrderTimeoutAlert)
            orderEventStream.process(new OrderPayMatch)
        timeoutData.print()
        timeoutData.getSideOutput(orderTimeoutOutputTag).print()
        env.execute()
    }

    class OrderTimeoutAlert() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
        //定义一个标记位，用于判断pay事件是否来过
        lazy val isPayedState: ValueState[Boolean] =
            getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))

        override def processElement(value: OrderEvent,
                                    ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                    out: Collector[OrderResult]): Unit = {
            val isPayed: Boolean = isPayedState.value()
            if (value.eventType == "create" && !isPayed)
                ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
            else if (value.eventType == "pay")
                isPayedState.update(true)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                             out: Collector[OrderResult]): Unit = {
            if (!isPayedState.value())
                out.collect(OrderResult(ctx.getCurrentKey, "order result"))
            isPayedState.clear()
        }
    }

    // TODO: 实现匹配和超时的处理函数
    class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
        lazy val isPayedState: ValueState[Boolean] =
            getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))
        lazy val timerState: ValueState[Long] =
            getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))

        override def processElement(value: OrderEvent,
                                    ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                    out: Collector[OrderResult]): Unit = {
            val isPayed: Boolean = isPayedState.value()
            val timerTS: Long = timerState.value()
            if (value.eventType == "create") {
                if (isPayed) {
                    out.collect(OrderResult(value.orderId, "payed successfully"))
                    ctx.timerService().deleteEventTimeTimer(timerTS)
                    isPayedState.clear()
                    timerState.clear()
                } else {
                    val ts: Long = value.eventTime * 1000L + 15 * 60 * 1000L
                    ctx.timerService().registerEventTimeTimer(ts)
                    timerState.update(ts)
                }
            } else if (value.eventType == "pay") {
                if (timerTS > 0) {
                    if (value.eventTime * 1000L < timerTS) //如果小于定时器时间，正常匹配
                        out.collect(OrderResult(value.orderId, "payed successfully"))
                    else
                        ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
                    //清空状态
                    isPayedState.clear()
                    timerState.clear()
                } else { //有支付但是还没有下单
                    isPayedState.update(true)
                    ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
                    timerState.update(value.eventTime * 1000L)
                }
            }
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                             out: Collector[OrderResult]): Unit = {
            if (isPayedState.value() && timerState.value() == timestamp)
                ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not create"))
            else if (!isPayedState.value())
                ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }

    }

}
