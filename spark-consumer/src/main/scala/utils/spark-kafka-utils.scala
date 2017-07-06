package com.sparkmovie.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SparkKafkaUtils {

    def createProducer(brokers : String) : KafkaProducer[String, String] = {
        val props = new Properties()
        props.put("bootstrap.servers", brokers)
        //props.put("client.id", "ScalaProducerExample")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        return new KafkaProducer[String, String](props)
    }

}
