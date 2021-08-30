package pulsar

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{Consumer, Message, MessageId, MessageListener, PulsarClient, Reader, ReaderListener, SubscriptionInitialPosition, SubscriptionMode, SubscriptionType}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.pulsar.common.api.proto.MessageIdData

import java.io.{File, FileInputStream}
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.StdIn
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

object Consumer  {

  def main(args: Array[String]): Unit = {

    val id = args(0)

    println(s"id: $id\n")

    val client = PulsarClient.builder()
      .serviceUrl(s"pulsar://localhost:6650")
      .build()

    /*val consumer = client.newConsumer()
      .topic("demo")
      .consumerName("c0")
      .subscriptionName("my-sub")
      .subscriptionMode(SubscriptionMode.Durable)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .messageListener(new MessageListener[Array[Byte]] {
        override def received(consumer: Consumer[Array[Byte]], msg: Message[Array[Byte]]): Unit = {

          println(s"new message: ${new String(msg.getValue)}")
          consumer.acknowledge(msg.getMessageId)

        }
      })
      .subscribeAsync().get()*/

    val topic = s"persistent://public/default/log3"

    //val file = reflect.io.File("pos")

    /*import org.apache.pulsar.client.admin.PulsarAdmin
    val url = "http://localhost:8080"
    // Pass auth-plugin class fully-qualified name if Pulsar-security enabled
    val authPluginClassName = "pulsar"
    // Pass auth-param if auth-plugin class requires it
    val authParams = "param1=value1"
    val useTls = false
    val tlsAllowInsecureConnection = true
    val tlsTrustCertsFilePath = null
    val admin = PulsarAdmin.builder()
      //authentication(authPluginClassName, authParams)
      .serviceHttpUrl(url)
      .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
      .allowTlsInsecureConnection(tlsAllowInsecureConnection).build()

    val topics = admin.topics()*/

    val mid =
    MessageId.earliest
    // DefaultImplementation.newMessageIdFromByteArray(file.inputStream().readAllBytes())
     // DefaultImplementation.newMessageIdFromByteArray(topics.getLastMessageId(topic).toByteArray)

    /*val consumer = client.newReader()
      .topic(topic)
      .readerName(s"r$id")
      .subscriptionName(s"sub$id")
      .startMessageId(mid)
      .startMessageIdInclusive()
      .readerListener(new ReaderListener[Array[Byte]] {
        override def received(reader: Reader[Array[Byte]], msg: Message[Array[Byte]]): Unit = {
          println(s"${Console.GREEN_B}new message: ${new String(msg.getValue)} id: ${msg.getSequenceId} ${msg.getTopicName}${Console.RESET}")
        }
      })
      .createAsync().get()*/
    //.create()

    val consumer = client.newConsumer()
      .topic(topic)
      .subscriptionName(s"log3-$id")
      //.subscriptionMode(SubscriptionMode.Durable)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .messageListener(new MessageListener[Array[Byte]] {
        override def received(consumer: Consumer[Array[Byte]], msg: Message[Array[Byte]]): Unit = {
          println(s"${Console.GREEN_B}new message: ${new String(msg.getValue)} id: ${msg.getSequenceId} ${msg.getTopicName}${Console.RESET}")
          consumer.acknowledge(msg.getMessageId)
        }
      })
      .subscribeAsync().get()

    //consumer.seek(DefaultImplementation.newMessageIdFromByteArray("id: 520:99:-1:0".getBytes()))

    /*while(true){
      val msg = consumer.readNext()
      println(s"${Console.GREEN_B}new message: ${new String(msg.getValue)} id: ${msg.getSequenceId} ${msg.getTopicName}${Console.RESET}")
     // file.outputStream(false).write(msg.getMessageId.toByteArray)
    }*/

    //consumer.seek(from)

    //StdIn.readLine()

    while(true){}

    consumer.close()
    client.shutdown()

    /*consumer.batchReceiveAsync().asScala.onComplete {
      case Success(messages) =>

        println(messages.asScala.map(s => new String(s.getValue)))

        consumer.close()
        client.close()

      case Failure(ex) =>

        ex.printStackTrace()
        consumer.close()
        client.close()

    }*/
  }

}

