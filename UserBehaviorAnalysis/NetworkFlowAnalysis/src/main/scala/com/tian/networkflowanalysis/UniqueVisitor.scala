package com.tian.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author tian
 * @date 2019/10/25 10:49
 * @version 1.0.0
 */
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        val resourcesPath = getClass.getResource("/UserBehaviorTest.csv")
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream = env
            .readTextFile(resourcesPath.getPath)
            .map(line => {
                val linearray = line.split(",")
                UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv")
            .timeWindowAll(Time.seconds(60 * 60))
            .apply(new UvCountByWindow())
            .print()

        env.execute("Unique Visitor Job")
    }
}

class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow,
                       input: Iterable[UserBehavior],
                       out: Collector[UvCount]): Unit = {

        val s: collection.mutable.Set[Long] = collection.mutable.Set()
        var idSet = Set[Long]()

        for ( userBehavior <- input) {
            idSet += userBehavior.userId
        }

        out.collect(UvCount(window.getEnd, idSet.size))
    }
}

