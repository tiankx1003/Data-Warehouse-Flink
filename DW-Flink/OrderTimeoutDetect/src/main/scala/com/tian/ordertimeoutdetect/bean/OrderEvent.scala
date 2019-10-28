package com.tian.ordertimeoutdetect.bean

/**
 * 订单事件样例类
 *
 * @author tian
 * @date 2019/10/28 9:23
 * @version 1.0.0
 */
case class OrderEvent(orderId: Long,
                      eventType: String,
                      eventTime: Long)
