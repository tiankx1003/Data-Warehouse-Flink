package com.tian.marketanalysis

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
 * @author tian
 * @date 2019/10/24 14:43
 * @version 1.0.0
 */
case class MarketingUserBehavior(userId: Long, behavior: String, channel: String, timestamp: Long)


class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

    var running: Boolean = true
    val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
    val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
    val rand: Random = Random

    override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
        val maxElements: Long = Long.MaxValue //最大生成数据的量
        var count: Long = 0L

        while (running && count < maxElements) {
            val id: Long = UUID.randomUUID().toString.toLong
            val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
            val channel: String = channelSet(rand.nextInt(channelSet.size))
            val ts: Long = System.currentTimeMillis()

            ctx.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
            count += 1
            TimeUnit.MILLISECONDS.sleep(5L)
        }
    }

    override def cancel(): Unit = running = false
}

