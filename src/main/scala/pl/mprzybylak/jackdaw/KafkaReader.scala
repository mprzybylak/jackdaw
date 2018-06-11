package pl.mprzybylak.jackdaw

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class KafkaReader(bootstrapServers:String = "localhost:1234")(topicName: String, topics: Seq[String]) {

  val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("group.id", "jackdaw-client")
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  props.put("max.poll.record", "1")

  val consumer = new KafkaConsumer[String, String](props)


  def read(messageToReadPerPartition: Long): Iterable[MessageInfo] = {


    // reset position to the end - little bit hacky
    val t: mutable.Map[String, util.List[PartitionInfo]] = consumer.listTopics().filter(t => t._1 == topicName)


    val messages = mutable.HashMap.empty[String, ListBuffer[MessageInfo]]

    for((topicName,partitionInfoes) <- t) {
      partitionInfoes.foreach {
        partitionInfo =>
          consumer.assign(Seq(new TopicPartition(topicName, partitionInfo.partition())))
          val parititonEnd = consumer.position(new TopicPartition(topicName, partitionInfo.partition()))
          val paritionStart = partitionStart(messageToReadPerPartition, parititonEnd)
          for (i <- 0 to paritionStart.asInstanceOf[Number].intValue()) {
            consumer.seek(new TopicPartition(topicName, partitionInfo.partition()), parititonEnd - i)

            val tMI = toMessageInfo(consumer.poll(1000))
//            println(tMI)

            tMI match {
              case Some(m) => {
                println(m)
                messages.get(topicName) match {
                  case Some(msgs) => messages += (topicName -> (msgs += m))
                  case None => messages += (topicName -> ListBuffer(m))
                }
              }
              case None => {
                println("Nima ;(")
              }
            }
          }
      }
    }

    println(messages)


    consumer.close()
    List()
  }

  def toMessageInfo(records: ConsumerRecords[String, String]): Option[MessageInfo] = {
    records.headOption.map(m => MessageInfo(m.key(), m.value(), m.topic(), m.partition(), m.offset(), m.timestamp(), m.timestampType().toString))
  }

  private def partitionStart(messageToReadPerPartition: Long, parititonEnd: Long) = {
    if (parititonEnd - messageToReadPerPartition < 0) 0.asInstanceOf[Number].longValue else parititonEnd - messageToReadPerPartition
  }
}

case class MessageInfo(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Long, timestampType: String)
