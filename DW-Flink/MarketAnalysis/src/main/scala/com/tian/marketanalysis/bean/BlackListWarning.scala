package com.tian.marketanalysis.bean

/**
 * 侧输出流的黑名单报警信息样例类
 *
 * @param userId 用户id
 * @param adId   广告id
 * @param msg    报警信息
 * @author tian
 * @date 2019/10/26 9:18
 * @version 1.0.0
 */
case class BlackListWarning(userId: Long,
                            adId: Long,
                            msg: String)
