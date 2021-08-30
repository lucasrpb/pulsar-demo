package pulsar

import org.apache.pulsar.client.api.{PulsarClient, Schema}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class PulsarProducer extends AnyFlatSpec {

  "it " should " produce successfully" in {

    val topic = s"persistent://public/default/log3"

    val client = PulsarClient.builder()
      .serviceUrl(s"pulsar://localhost:6650")
      .build()

    val producer = client.newProducer()
      .topic(topic)
      .producerName("p0")
      .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
      .sendTimeout(10, TimeUnit.SECONDS)
      .blockIfQueueFull(true)
      .createAsync().get()

    for(i<-0 until 100){
      producer.send(s"Hello world-${UUID.randomUUID().toString}".getBytes())
    }

    /*producer.sendAsync(s"Hello World ${UUID.randomUUID().toString}".getBytes()).toCompletableFuture.asScala.onComplete {
      case Success(value) =>

        producer.close()
        client.close()

      case Failure(ex) =>

        ex.printStackTrace()
        producer.close()
        client.close()

    }*/

    producer.close()
    client.close()

  }

}

