package com.tian.marketanalysis.bean

/**
 * 输出按照省份划分的点击统计结果
 *
 * @param windowEnd
 * @param province
 * @param count
 * @author tian
 * @date 2019/10/26 9:17
 * @version 1.0.0
 */
case class CountByProvince(windowEnd: String,
                           province: String,
                           count: Long)
