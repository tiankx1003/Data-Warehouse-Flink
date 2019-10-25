package com.tian.networkanalysis.bean

/**
 * @author tian
 * @date 2019/10/25 8:47
 * @version 1.0.0
 */
case class ApacheLogEvent(ip: String,
                          userId: String,
                          eventTime: Long,
                          method: String,
                          url: String)
