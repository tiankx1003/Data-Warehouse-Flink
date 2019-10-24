package com.tian.hotitemsanalysis

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.BufferedSource

/**
 * 采集数据到Kafka
 *
 * @author tian
 * @date 2019/10/24 16:10
 * @version 1.0.0
 */
object KafkaProducer {

    def writeToKafka(topic: String): Unit = {
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
        val bufferedSource: BufferedSource = io.Source.fromFile("files/UserBehavior.csv")
        for (line <- bufferedSource.getLines()) {
            val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
            producer.send(record)
        }
        producer.close()
    }

    def main(args: Array[String]): Unit = {
        writeToKafka("hotitems")
    }
}
