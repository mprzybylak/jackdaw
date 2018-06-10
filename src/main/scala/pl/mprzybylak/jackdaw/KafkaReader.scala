package pl.mprzybylak.jackdaw

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._

class KafkaReader(topicName: String) {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:12345")
  props.put("group.id", "consumer-tutorial")

  import org.apache.kafka.clients.consumer.ConsumerConfig

  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)

  val consumer = new KafkaConsumer[String, String](props)

  def read(): String = {
    consumer.subscribe(util.Arrays.asList(topicName))
    consumer.poll(1) // hack ftw


    consumer.seekToBeginning(consumer.assignment())
    val msg = consumer.poll(1000)

    consumer.close()
    msg.head.value()
  }

}
