package com.tian.ordertimeoutdetect.bean

/**
 * @author tian
 * @date 2019/10/28 11:38
 * @version 1.0.0
 */
case class OrderEventWithTxId(orderId: Long,
                              eventType: String,
                              txId: String,
                              eventTime: Long)
