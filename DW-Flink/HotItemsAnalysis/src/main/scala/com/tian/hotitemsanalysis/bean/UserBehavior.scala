package com.tian.hotitemsanalysis.bean

/**
 * 用户行为样例类
 *
 * @param userId 用户编号
 * @param itemId 货物编号
 * @param categoryId 品类编号
 * @param behavior 用户行为
 * @param timestamp 时间戳
 * @author tian
 * @date 2019/10/24 20:52
 * @version 1.0.0
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)
