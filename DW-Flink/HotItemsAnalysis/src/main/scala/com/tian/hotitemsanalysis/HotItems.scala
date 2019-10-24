package com.tian.hotitemsanalysis

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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
        sourceData.map(line=>{
            val splits: Array[String] = line.split(",")
            
        })
    }
}
