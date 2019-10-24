package com.tian.ordertimeout

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/24 18:44
 * @version 1.0.0
 */
case class OrderEvent2(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatch {

    val unmatchedPays: OutputTag[OrderEvent2] = new OutputTag[OrderEvent2]("unmatchedPays")
    val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatchedReceipts")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val OrderEvent2Stream: KeyedStream[OrderEvent2, String] = env.readTextFile("files/OrderLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                OrderEvent2(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
            })
            .filter(_.txId != "")
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)

        val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("files/ReceiptLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
            })
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)

        val processedStream: DataStream[(OrderEvent2, ReceiptEvent)] = OrderEvent2Stream
            .connect(receiptEventStream)
            .process(new TxMatchDetection)

        processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
        processedStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")

        processedStream.print("processed")

        env.execute()
    }

    class TxMatchDetection extends CoProcessFunction[OrderEvent2, ReceiptEvent, (OrderEvent2, ReceiptEvent)] {
        lazy val payState: ValueState[OrderEvent2] =
            getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent2]("pay-state", classOf[OrderEvent2]))
        lazy val receiptState: ValueState[ReceiptEvent] =
            getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

        override def processElement1(pay: OrderEvent2,
                                     ctx: CoProcessFunction[OrderEvent2, ReceiptEvent, (OrderEvent2, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEvent2, ReceiptEvent)]): Unit = {
            val receipt: ReceiptEvent = receiptState.value()

            if (receipt != null) {
                receiptState.clear()
                out.collect((pay, receipt))
            } else {
                payState.update(pay)
                ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L)
            }
        }

        override def processElement2(receipt: ReceiptEvent,
                                     ctx: CoProcessFunction[OrderEvent2, ReceiptEvent, (OrderEvent2, ReceiptEvent)]#Context,
                                     out: Collector[(OrderEvent2, ReceiptEvent)]): Unit = {
            val payment: OrderEvent2 = payState.value()

            if (payment != null) {
                payState.clear()
                out.collect((payment, receipt))
            } else {
                receiptState.update(receipt)
                ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L)
            }
        }

        override def onTimer(timestamp: Long,
                             ctx: CoProcessFunction[OrderEvent2, ReceiptEvent, (OrderEvent2, ReceiptEvent)]#OnTimerContext,
                             out: Collector[(OrderEvent2, ReceiptEvent)]): Unit = {
            if (payState.value() != null) {
                ctx.output(unmatchedPays, payState.value())
            }
            if (receiptState.value() != null) {
                ctx.output(unmatchedReceipts, receiptState.value())
            }
            payState.clear()
            receiptState.clear()
        }
    }
}

