package kafka

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

// we will need to have zookeepr and kafka-server running for these to work
object Consumer {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("quick-start")
  }

  def consumeFromKafka(topic: String) = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("auto.offset.reset", "latest")

    props.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {
      val record = consumer.poll(Duration.ofMillis(1000)).asScala
      for (data <- record.iterator) {
        println(data.key()+"->"+data.value())
      }
    }

  }

}
