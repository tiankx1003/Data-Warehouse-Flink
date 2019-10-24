package com.tian.hotitemsanalysis.bean

/**
 * 商品浏览统计
 *
 * @param itemId    商品编号
 * @param windowEnd 窗口结束位置
 * @param count     计数
 * @author tian
 * @date 2019/10/24 20:56
 * @version 1.0.0
 */
case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)
