package pl.mprzybylak.jackdaw

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._

class KafkaReader(bootstrapServers:String = "localhost:1234")(topicName: String) {

  val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("group.id", "jackdaw-client")
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)

  val consumer = new KafkaConsumer[String, String](props)

  def read(): Iterable[MessageInfo] = {
    consumer.subscribe(util.Arrays.asList(topicName))
    consumer.poll(1) // hack ftw
    consumer.seekToBeginning(consumer.assignment())

    val msg = consumer.poll(1000)

    consumer.close()

    msg.map(m => MessageInfo(m.key(), m.value(), m.topic(), m.partition(), m.offset(), m.timestamp(), m.timestampType().toString))
  }

}

case class MessageInfo(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Long, timestampType: String)