package com.tian.loginfaildetect.bean

/**
 *
 * @param userId        用户编号
 * @param firstFailTime 第一次失败时间
 * @param lastFailTime  第二次失败时间
 * @param WarningMsg    报警信息
 * @author tian
 * @date 2019/10/26 10:52
 * @version 1.0.0
 */
case class Warning(userId: Long,
                   firstFailTime: Long,
                   lastFailTime: Long,
                   WarningMsg: String)
