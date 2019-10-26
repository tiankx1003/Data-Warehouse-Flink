package com.tian.marketanalysis.bean

/**
 *
 * @param userId
 * @param adId
 * @param province
 * @param city
 * @param ts
 * @author tian
 * @date 2019/10/26 9:15
 * @version 1.0.0
 */
case class AdClickLog(userId: Long,
                       adId: Long,
                       province: String,
                       city: String,
                       ts: Long)
