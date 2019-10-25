package com.tian.networkanalysis.bean

/**
 * 输入用户行为的样例类
 *
 * @param userId     用户id
 * @param itemId     商品id
 * @param categoryId 品类id
 * @param behavior   用户行为
 * @param ts         时间戳
 * @author tian
 * @date 2019/10/25 19:24
 * @create 2019-10-25 19:31:55
 * @version 1.0.0
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        ts: Long)
