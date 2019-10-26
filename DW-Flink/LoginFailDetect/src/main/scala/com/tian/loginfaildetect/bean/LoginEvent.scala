package com.tian.loginfaildetect.bean

/**
 * @param userId    用户id
 * @param ip        IP地址
 * @param status 时间类型
 * @param eventTime 事件事件戳
 * @author tian
 * @date 2019/10/26 10:47
 * @version 1.0.0
 */
case class LoginEvent(userId: Long,
                      ip: String,
                      status: String,
                      eventTime: Long)
