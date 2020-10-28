package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer{
  def main(args: Array[String]): Unit = {
    writeToKafka("quick-start")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    for( i <- 1 to 10){
      val record = new ProducerRecord[String, String](topic, "key"+i, "value"+i)
      producer.send(record)
      println(record.key()+"->"+record.value())
    }
    producer.close()
  }
}
