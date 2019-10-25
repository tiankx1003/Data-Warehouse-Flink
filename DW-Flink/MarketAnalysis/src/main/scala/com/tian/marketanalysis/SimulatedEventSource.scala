package com.tian.marketanalysis

import com.tian.marketanalysis.bean.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * 自定义source
 *
 * @author tian
 * @date 2019/10/25 9:38
 * @version 1.0.0
 */
object SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {
    var isRunning = true //用于标识是否在运行
    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = ???

    override def cancel(): Unit = isRunning = false
}
