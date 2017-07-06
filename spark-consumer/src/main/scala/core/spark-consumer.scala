package com.sparkmovie.core

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.rdd._

import play.api.libs.json._

import com.sparkmovie.utils.CommandLineParser
import com.sparkmovie.utils.MovieUtils
import com.sparkmovie.utils.SparkKafkaUtils

object SparkConsumer {

    val TOPICS = Map[String, String] (
        "RAW_MOVIE_READ" -> "movie-analyzed",
        "MOVIE_WRITE" -> "movie-processed"
    )

    def main(args: Array[String]) {

        val requiredOptions = List(
            "brokers",
            "group-id"
        )
        val topics = Array(
            TOPICS.get("RAW_MOVIE_READ").get
        )
        
        System.out.println("\n[SPARK CONSUMER] Starting Spark instance...")
        val options = CommandLineParser.parseCmdLine(Map(), args.toList)
        for (x <- requiredOptions) {
            if (!options.contains(x)) {
                System.err.println("[SPARK CONSUMER] Missing argument: <" + x + ">")
                System.out.println("[SPARK CONSUMER] Stoping Spark instance...")
                System.exit(1)
            }
        }

        val kafkaParams = Map[String, Object] (
            "bootstrap.servers" -> options.get("brokers").get,
            "group.id" -> options.get("group-id").get,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer]
        )

        val sparkConf = new SparkConf()
                                .setAppName("SparkConsumer")
                                .setMaster("local[*]")

        // Creates the streaming context allowing to connect to Kafka.
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        ssc.checkpoint("checkpoint")

        // Creates the stream connecting the streaming context
        // to several Kafka topics.
        val stream = KafkaUtils.createDirectStream[String, String] (
            ssc, PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        stream.map(record => (record.key, record.value))
        stream.foreachRDD { rdd =>
            
            rdd.foreachPartition(partition => {
                val producer = SparkKafkaUtils
                                .createProducer(options.get("brokers").get)

                partition.foreach {
                  
                    case movieConsumer : ConsumerRecord[String, String] => {

                        Json.parse(movieConsumer.value())
                            .validate[MovieUtils.MovieRaw] match {
                                case JsSuccess(movie, _) => {
                                    val jsonMovie = Some(movie)
                                    // Analyze movie
                                    // Send to kafka the new movie class
                                    // val message = new ProducerRecord[String, String](TOPICS.get("MOVIE_WRITE").get, null, "CACAAAAAAA")
                                    // producer.send(message)
                                }
                                case JsError(_) => {
                                    println("Failed to process")
                                    None
                                }
                            }

                    }

                }

                producer.flush()
                producer.close()

            })
        }
        //}

        ssc.start()
        ssc.awaitTermination()
    }

    def convertToJSON(rdd : RDD[String]) : RDD[Option[MovieUtils.MovieRaw]] = {
        rdd.map(x => {
            Json.parse(x).validate[MovieUtils.MovieRaw] match {
                case JsSuccess(movie, _) => {    
                    Some(movie)
                }
                case JsError(_) => {
                    println("Failed to process")
                    None
                }
            }
        })
    }

}
