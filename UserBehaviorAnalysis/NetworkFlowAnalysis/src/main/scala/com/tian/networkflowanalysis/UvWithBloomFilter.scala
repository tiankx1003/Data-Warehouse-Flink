package com.tian.networkflowanalysis

import java.lang
import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @author tian
 * @date 2019/10/25 11:44
 * @version 1.0.0
 */
object UvWithBloomFilter {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val resourcesPath: URL = getClass.getResource("/UserBehaviorTest.csv")
        val stream: DataStream[UvCount] = env
            .readTextFile(resourcesPath.getPath)
            .map(data => {
                val dataArray: Array[String] = data.split(",")
                UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv")
            .map(data => ("dummyKey", data.userId))
            .keyBy(_._1)
            .timeWindow(Time.seconds(60 * 60))
            .trigger(new MyTrigger()) // 自定义窗口触发规则
            .process(new UvCountWithBloom()) // 自定义窗口处理规则

        stream.print()
        env.execute("Unique Visitor with bloom Job")
    }
}

// 自定义触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    }

    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        // 每来一条数据，就触发窗口操作并清空
        TriggerResult.FIRE_AND_PURGE
    }
}

// 自定义窗口处理函数
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    // 创建redis连接
    lazy val jedis: Jedis = new Jedis("localhost", 6379)
    lazy val bloom: Bloom = new Bloom(1 << 29)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {
        val storeKey: String = context.window.getEnd.toString
        var count: Long = 0L
        if (jedis.hget("count", storeKey) != null) {
            count = jedis.hget("count", storeKey).toLong
        }

        val userId: String = elements.last._2.toString
        val offset: Long = bloom.hash(userId, 61)

        val isExist: lang.Boolean = jedis.getbit(storeKey, offset)
        if (!isExist) {
            jedis.setbit(storeKey, offset, true)
            jedis.hset("count", storeKey, (count + 1).toString)
            out.collect(UvCount(storeKey.toLong, count + 1))
        } else {
            out.collect(UvCount(storeKey.toLong, count))
        }
    }
}

// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
    private val cap: Long = size

    def hash(value: String, seed: Int): Long = {
        var result: Int = 0
        for (i <- 0 until value.length) {
            // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
            result = result * seed + value.charAt(i)
        }
        (cap - 1) & result
    }
}

