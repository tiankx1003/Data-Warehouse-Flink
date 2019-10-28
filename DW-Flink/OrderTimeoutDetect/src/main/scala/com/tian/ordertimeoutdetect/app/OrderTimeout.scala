package com.tian.ordertimeoutdetect.app

import java.util

import com.tian.ordertimeoutdetect.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author tian
 * @date 2019/10/28 9:26
 * @version 1.0.0
 */
object OrderTimeout {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // TODO: 考虑使用ProcessingTime语义

        val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile("files/OrderLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
            })
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.orderId) //针对每个订单而言

        val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
            .where(_.eventType == "create")
            .followedBy("follow")
            .where(_.eventType != "")
            .within(Time.minutes(15))
        //应用pattern
        val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPattern)
        //定义侧输出流标签
        val orderTimeoutOutputTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")
        val result: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())
        result.print()
        env.execute()
    }

    class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
        /**
         *
         * @param map
         * @param l 超时时间戳
         * @return
         */
        override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
            val timeoutOrderId: Long = map.get("begin").iterator().next.orderId
            OrderResult(timeoutOrderId, "time out")
        }
    }

    class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
        override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
            val payOrderId: Long = map.get("follow").iterator().next().orderId
            OrderResult(payOrderId, "pay")
        }
    }

}
