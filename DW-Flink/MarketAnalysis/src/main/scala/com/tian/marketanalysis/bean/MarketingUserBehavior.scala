package com.tian.marketanalysis.bean

/**
 * 定义源数据样例类
 *
 * @param userId   用户id
 * @param behavior 用户行为
 * @param channel  渠道
 * @param ts       时间戳
 * @author tian
 * @date 2019/10/25 21:10
 * @version 1.0.0
 */
case class MarketingUserBehavior(userId: String,
                                 behavior: String,
                                 channel: String,
                                 ts: Long)
