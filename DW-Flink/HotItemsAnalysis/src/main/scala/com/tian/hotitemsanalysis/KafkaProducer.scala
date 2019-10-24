package com.tian.hotitemsanalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * 从文件读取数据到Kafka
 *
 * @author tian
 * @date 2019/10/24 20:40
 * @version 1.0.0
 */
object KafkaProducer {
    def main(args: Array[String]): Unit = {
        writeToKafka("hotitems")
    }

    def writeToKafka(topic: String): Unit = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](properties)
        val bufferedSource = io.Source.fromFile("file/UserBehaviors.csv")
        for (line <- bufferedSource.getLines()) {
            val record = new ProducerRecord[String, String](topic, line)
            producer.send(record)
        }
        producer.close()
    }
}
