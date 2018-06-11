package pl.mprzybylak.jackdaw

import java.util.UUID

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{Matchers, WordSpec}

class KafkaReaderTest extends WordSpec with EmbeddedKafka with Matchers {

  "kafka consumer" should {

    "read from topic" in {

      val prop = Map("unclean.leader.election.enable" -> "true",
        "min.insync.replicas" -> "1")
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 1234, customBrokerProperties = prop)

      withRunningKafka {

        val topicName = "topic"
        val secondTopic = "secondTopic"

        createCustomTopic(topicName, Map[String, String](), 3, 1): Unit
        createCustomTopic("temp", Map[String, String](), 5, 1): Unit
        implicit val serializer = new StringSerializer

        for (i <- 1 to 10) {
          publishToKafka[String, String](topicName, s"key-$i", message())
          Thread.sleep(2)
          publishToKafka[String, String](secondTopic, s"secondKey-$i", message())
          Thread.sleep(10)

        }

        val consumer = new KafkaReader()(topicName, Seq(secondTopic))

        println(consumer.read(3).mkString("\n"))
      }
    }

  }

  private def message() = {
    UUID.randomUUID().toString
  }
}
