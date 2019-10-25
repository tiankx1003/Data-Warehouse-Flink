package com.tian.networkanalysis.app

import java.lang

import com.tian.networkanalysis.bean.{UserBehavior, UvCount}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.util.hashing.MurmurHash3

/**
 * 使用布隆过滤器的访客数统计
 *
 * @author tian
 * @date 2019/10/25 20:07
 * @version 1.0.0
 */
object UvWithBloom {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(8)
        val fileSourceData: DataStream[String] = env.readTextFile("files/UserBehavior.csv")
        val behaviorData: DataStream[UserBehavior] = fileSourceData
            .map(data => {
                val dataArr: Array[String] = data.split(",")
                UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
            })
        //对数据进行聚合窗口处理
        val resultData: DataStream[UvCount] = behaviorData.filter(_.behavior == "pv")
            .map(data => ("pv", data.userId))
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .trigger(new MyTrigger())
            .process(new UvCountWithBloom())
        resultData.print()
        env.execute()
    }
}

/**
 * 自定义窗口触发机制，每来一条数据都会触发一次窗口操作，写入到redis中
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = { // TODO: ?
        // val myState: ValueState[Int] = ctx.getPartitionedState(new ValueStateDescriptor[Int]("my-state",classOf[Int]))
        // myState.clear()
    }
}

/**
 * 自定义process function
 */
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    lazy val jedis: Jedis = new Jedis("localhost", 6379)
    lazy val bloom: Bloom = new Bloom(1 << 28) //32MB位图
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {
        //在redis里存储位图，以windowEnd作为key存储，另外，storeKey也作为hash表中的key
        val storeKey: String = context.window.getEnd.toString
        var count: Long = 0L //把当前窗口uv的count值也存入redis表中，名为count的hash表
        if (jedis.hget("count", storeKey) != null) //获取当前的count值
            count = jedis.hget("count", storeKey).toLong
        //根据hash值，查对应偏移量的位是否有值，说明当前user是否存在
        val userId: String = elements.last._2.toString
        val offset: Long = bloom.hash(userId, 61)
        val isExist: lang.Boolean = jedis.getbit(storeKey, offset)
        if (!isExist) { //如果不存在，将位图中对应的位置置1, count+1
            jedis.setbit(storeKey, offset, true)
            jedis.hset("count", storeKey, (count + 1).toString)
            out.collect(UvCount(storeKey.toLong, count + 1))
        }
    }
}

/**
 * 自定义一个布隆过滤器
 *
 * @param size 传入bit数，要求为正整数
 */
class Bloom(size: Long) extends Serializable {
    private val cap: Long = size

    //使用hash函数实现userId到每个位的对应关系
    def hash(value: String, seed: Int): Long = {
        // MurmurHash3.stringHash(value)
        var result: Int = 0
        for (elem <- 0 until value.length)
            result = result * seed + value.charAt(elem)
        (cap - 1) & result
    }
}