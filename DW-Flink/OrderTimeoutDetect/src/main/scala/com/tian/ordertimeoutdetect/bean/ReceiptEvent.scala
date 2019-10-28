package com.tian.ordertimeoutdetect.bean

/**
 * 收据事件
 *
 * @author tian
 * @date 2019/10/28 11:39
 * @version 1.0.0
 */
case class ReceiptEvent(txId: String,
                        payChannel: String,
                        eventTime: Long)
