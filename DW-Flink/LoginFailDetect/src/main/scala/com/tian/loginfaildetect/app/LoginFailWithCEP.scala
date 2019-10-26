package com.tian.loginfaildetect.app

import java.util

import com.tian.loginfaildetect.bean.{LoginEvent, Warning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author tian
 * @date 2019/10/26 15:13
 * @version 1.0.0
 */
object LoginFailWithCEP {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        //读取数据
        val loginEventData: DataStream[LoginEvent] = env.readTextFile("files/LoginLog.csv")
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
            })
        val keyedStreamData: KeyedStream[LoginEvent, Long] = loginEventData
            .keyBy(_.userId)

        //定义模式
        val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
            .begin[LoginEvent]("start").where(_.status == "fail") //定义第一个失败事件模式
            .next("next").where(_.status == "fail") //第二个登录失败的事件
            .within(Time.seconds(5)) //五秒之内

        //将模式应用到数据流上
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyedStreamData, loginFailPattern)

        //从pattern stream中检出符合规则的事件序列做处理
        val loginFailWarningStream: DataStream[Warning] = patternStream.select(new LoginFailDetect)

        loginFailWarningStream.print()
        env.execute("login fail with CEP")
    }

    class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {
        override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
            // TODO: 一般是循环的需要对迭代器进行遍历，这里的数据只有一条
            val firstFailEvent: LoginEvent = map.get("start").iterator().next()
            val secondFailEvent: LoginEvent = map.get("next").iterator().next()
            Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail")
        }
    }

}
