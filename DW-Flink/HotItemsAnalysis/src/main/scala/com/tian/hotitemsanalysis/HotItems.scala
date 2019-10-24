package com.tian.hotitemsanalysis

import java.util.Properties

import com.tian.hotitemsanalysis.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @author tian
 * @date 2019/10/24 20:35
 * @version 1.0.0
 */
object HotItems {
    def main(args: Array[String]): Unit = {
        //Kafka配置
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //引入EventTime
        env.setParallelism(8) //设置并行度
        import org.apache.flink.streaming.api.scala._
        val sourceData: DataStream[String] =
            env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
        val behaviorData = sourceData.map(line => {
            val splits: Array[String] = line.split(",")
            UserBehavior(splits(0).toLong, splits(1).toLong, splits(2).toInt, splits(3), splits(4).toLong)
        })
        val resultStream = behaviorData
            .assignAscendingTimestamps(_.timestamp * 1000) //设置时间戳
            .filter(_.behavior == "pv") //过滤出对应指标
            .keyBy(_.itemId)
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new WindowResultFunction)
            .keyBy(1)
            .process(new TopNHotItems(3))
        resultStream


    }

    //统计count统计的聚合函数实现，没出现一条记录加一
    class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: UserBehavior, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    //用于输出窗口的结果
    class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
        override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit =
            out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }

    class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
        private var itemState: ListState[ItemViewCount] = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            //定义状态变量的类型
            val itemStateDesc: ListStateDescriptor[ItemViewCount] =
                new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
            //从运行时上下文中获取状态并赋值
            itemState = getRuntimeContext.getListState(itemStateDesc)
        }

        override def processElement(i: ItemViewCount,
                                    context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                                    collector: Collector[String]): Unit = {
            itemState.add(i) //每条数据都保存到状态中
            //
            context.timerService().registerEventTimeTimer(i.windowEnd + 1)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                             out: Collector[String]): Unit = {

        }
    }

}
