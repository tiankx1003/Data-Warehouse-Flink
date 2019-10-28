package com.tian.ordertimeoutdetect.app

import com.tian.ordertimeoutdetect.bean.{OrderEventWithTxId, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 两条流的订单交易匹配
 *
 * @author tian
 * @date 2019/10/28 11:37
 * @version 1.0.0
 */
object TxMatch {
    //定义侧输出流标签
    private val unmatchedPays: OutputTag[OrderEventWithTxId] = new OutputTag[OrderEventWithTxId]("unmatchedPays")
    private val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatchedReceipts")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //订单支付数据源
        val orderEventStream: KeyedStream[OrderEventWithTxId, String] = env.readTextFile("files/OrderLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                OrderEventWithTxId(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
            })
            .filter(_.txId != "")
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)

        //到账信息数据源
        val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("files/ReceiptLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
            })
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)
        //连接两条流，进行处理
        val processedStream: DataStream[(OrderEventWithTxId, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
            .process(new TxPayMatch())

        processedStream.print()
        env.execute()
    }

    class TxPayMatch() extends CoProcessFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)] {
        //定义状态用于保存已经来的事件
        lazy val payState: ValueState[OrderEventWithTxId] =
            getRuntimeContext.getState(new ValueStateDescriptor[OrderEventWithTxId]("payState", classOf[OrderEventWithTxId]))
        lazy val receiptState: ValueState[ReceiptEvent] =
            getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptState", classOf[ReceiptEvent]))

        override def processElement1(value: OrderEventWithTxId,
                                     ctx: CoProcessFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEventWithTxId, ReceiptEvent)]): Unit = {
            val receipt: ReceiptEvent = receiptState.value()
            if (receipt != null) { //如果已经有receipt到了，那么正常输出匹配
                out.collect((value, receipt))
                receiptState.clear()
            } else { //如果receipt还没到，那么保存pay进状态，注册一个定时器等待
                payState.update(value)
                ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
            }
        }

        override def processElement2(value: ReceiptEvent,
                                     ctx: CoProcessFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEventWithTxId, ReceiptEvent)]): Unit = {
            val pay: OrderEventWithTxId = payState.value()
            if (pay != null) { //如果已经有receipt到了，那么正常输出匹配
                out.collect((pay, value))
                receiptState.clear()
            } else { //如果receipt还没到，那么保存pay进状态，注册一个定时器等待
                receiptState.update(value)
                ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
            }
        }

        override def onTimer(timestamp: Long,
                             ctx: CoProcessFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)]#OnTimerContext,
                             out: Collector[(OrderEventWithTxId, ReceiptEvent)]): Unit = {
            if (payState != null) //如果payState没有清空说明对应的receipt没有到
                ctx.output(unmatchedPays, payState.value())
            if (receiptState != null) //如果receiptState没有清空说明对应的pay没有到
                ctx.output(unmatchedReceipts, receiptState.value())
        }
    }

}
