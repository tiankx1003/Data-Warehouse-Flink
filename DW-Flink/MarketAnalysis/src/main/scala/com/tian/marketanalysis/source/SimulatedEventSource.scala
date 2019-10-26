package com.tian.marketanalysis.source

import java.util.UUID

import com.tian.marketanalysis.bean.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

/**
 * 自定义source
 *
 * @author tian
 * @date 2019/10/25 9:38
 * @version 1.0.0
 */
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {
    var isRunning: Boolean = true //用于标识是否在运行
    //渠道的集合
    val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "XiaomiStore", "weibo", "wechat")
    //用户行为的集合
    val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOWD", "UPDATE", "INSTALL", "UNINSTALL")
    val rand: Random = Random

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
        val maxCount: Long = Long.MaxValue //最大生成数据的量
        var count: Long = 0L //用于标识偏移量
        while (isRunning && count < maxCount) {
            //随机生成字段
            val id: String = UUID.randomUUID().toString
            val behavior: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
            val channel: String = channelSet(rand.nextInt(channelSet.size))
            val ts: Long = System.currentTimeMillis()
            ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
            count += 1
            Thread.sleep(10L)
        }
    }

    override def cancel(): Unit = isRunning = false
}
