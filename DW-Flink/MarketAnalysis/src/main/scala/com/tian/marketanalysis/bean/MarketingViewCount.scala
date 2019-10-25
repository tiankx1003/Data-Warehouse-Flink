package com.tian.marketanalysis.bean

/**
 * 输出样例类
 *
 * @param windowStart 窗口起始
 * @param channel     渠道
 * @param behavior    用户行为
 * @param count       统计数目
 * @author tian
 * @date 2019/10/25 21:12
 * @version 1.0.0
 */
case class MarketingViewCount(windowStart: String,
                              channel: String,
                              behavior: String,
                              count: Long)
