package com.moviewriter.core

import java.util.concurrent.atomic._

import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.rdd._

import com.moviewriter.utils._

object Writer {

    def main(args: Array[String]) {
        
        val requiredOptions = List(
            "brokers",
            "group-id",
            "consume",
            "save-path"
        )

        System.out.println("\n[HDFS Writer] Starting instance...")
        val options = CommandLineParser.parseCmdLine(Map(), args.toList)
        for (x <- requiredOptions) {
            System.err.println("ARG = " + x)
            if (!options.contains(x)) {
                System.err.println("[HDFS Writer] Missing argument: <" + x + ">")
                System.err.println("[HDFS Writer] Stoping Spark instance...")
                System.exit(1)
            }
        }

        val topics = Array(options.get("consume").get)

        val kafkaParams = Map[String, Object] (
            "bootstrap.servers" -> options.get("brokers").get,
            "group.id" -> options.get("group-id").get,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer]
        )

        val sparkConf = new SparkConf()
                                .setAppName("SparkConsumer")
                                .setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        val ssc = new StreamingContext(sc, Seconds(1))
        val stream = KafkaUtils.createDirectStream[String, String] (
            ssc, PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // Allows to keep track of the file ID
        // avoiding to erase previously written content.
        val path = options.get("save-path").get + "/save"
        val i = new AtomicInteger(0)
        stream.map(record => record.value)
                .saveAsTextFiles(path, "backup")

        ssc.start()
        ssc.awaitTermination()
    
    }

}
