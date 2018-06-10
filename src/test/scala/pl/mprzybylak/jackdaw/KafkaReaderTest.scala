package pl.mprzybylak.jackdaw

import java.util.UUID

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.WordSpec
import org.scalatest.Matchers

class KafkaReaderTest extends WordSpec with EmbeddedKafka with Matchers {

  "kafka consumer" should {
    "read from topic" in {

      implicit val config = EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {

        val topicName = "topic"

        val expectedMessage = message()

        createCustomTopic(topicName, Map[String,String](), 1, 1): Unit
        publishStringMessageToKafka(topicName, expectedMessage)

        val consumer = new KafkaReader(topicName)

        consumer.read() shouldEqual expectedMessage
      }
    }
  }

  private def message() = {
    UUID.randomUUID().toString
  }
}
