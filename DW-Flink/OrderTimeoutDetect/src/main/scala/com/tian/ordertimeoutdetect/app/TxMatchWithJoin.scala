package com.tian.ordertimeoutdetect.app

import com.tian.ordertimeoutdetect.bean.{OrderEventWithTxId, ReceiptEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/28 16:53
 * @version 1.0.0
 */
object TxMatchWithJoin {
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

        //window join
        val windowJoinResult: DataStream[(OrderEventWithTxId, ReceiptEvent)] = orderEventStream.join(receiptEventStream)
            .where(_.txId)
            .equalTo(_.txId)
            .window(TumblingEventTimeWindows.of(Time.seconds(15)))
            .apply((order, receipt) => (order, receipt))
        windowJoinResult.print()

        //interval join
        orderEventStream.intervalJoin(receiptEventStream)
            .between(Time.seconds(-15), Time.seconds(20))
            .process(new TxMatchByIntervalJoin)

    }

    class TxMatchByIntervalJoin() extends ProcessJoinFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)] {
        override def processElement(left: OrderEventWithTxId,
                                    right: ReceiptEvent,
                                    ctx: ProcessJoinFunction[OrderEventWithTxId, ReceiptEvent, (OrderEventWithTxId, ReceiptEvent)]#Context,
                                    out: Collector[(OrderEventWithTxId, ReceiptEvent)]): Unit =
            out.collect((left, right))
    }

}
