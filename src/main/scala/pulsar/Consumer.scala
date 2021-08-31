package pulsar

import org.apache.pulsar.client.api.{AuthenticationFactory, Consumer, Message, MessageId, MessageListener, PulsarClient, SubscriptionInitialPosition, SubscriptionMode, SubscriptionType}

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

object Consumer  {

  def main(args: Array[String]): Unit = {

    val client = PulsarClient.builder()
      .serviceUrl(SERVICE_URL)
      /*.authentication(
        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjbGllbnQ7YjBmYWRjYjUtYTg0Ny00MTBlLWFiMGItY2RjZDYzOTQ0MThiO1pHRnlkMmx1WkdJPSJ9.XSUJyWnIs3vl7nBUyOfDvtaueVlvfoIxPVZf16Ne1UxM6pvFTSw1RmuhjSK87udQ-F3kCyZSEx5nJXOCoouxE2mmKDvM6OqWYwUEGcGjmsmmx5_p-Kbz_im3AJ14jSFrjUZ1J5Ly8T3OL3vwAKC1rT3MPIwarqBwJbowNqXEESI1NBI6L6njDNFJLDNOUn7rArk6_OturuqtGPX8_vgTmawB0uonfmmTxXNodewWOxsqEkXNGkOQwJ_KwmUNAcrj7qkY7VVVTdD7H7ykK4A3ipc54o8nuFjHXt_UhgyPFFKNDoIn9r402nYqD359wdX13Qkix9RmOn517b8PfeGZ3A")
      )*/
      .allowTlsInsecureConnection(true)
      .build()

    var l1 = Seq.empty[String]
    var l2 = Seq.empty[String]

    /*val r1 = client.newReader()
      .topic("darwindb/default/log1")
      .startMessageId(MessageId.earliest)
      .create()

    while(r1.hasMessageAvailable){
      val msg = r1.readNext()
      l1 = l1 :+ new String(msg.getData)
    }

    val r2 = client.newReader()
      .topic("darwindb/default/log1")
      .startMessageId(MessageId.earliest)
      .create()

    while(r2.hasMessageAvailable){
      val msg = r2.readNext()
      l2 = l2 :+ new String(msg.getData)
    }*/

    val c1 = client.newConsumer()
      .topic(TOPIC)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionName("c3")
      .subscribe()

    while(l1.length < 100){
      val msg = c1.receive()
      val str = new String(msg.getData)

      println(s"${Console.MAGENTA_B}$str${Console.RESET}")

      l1 = l1 :+ str

      //c1.acknowledge(msg.getMessageId)
    }

    val c2 = client.newConsumer()
      .topic(TOPIC)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionName("c4")
      .subscribe()

    while(l2.length < 100){
      val msg = c2.receive()
      val str = new String(msg.getData)

      println(s"${Console.GREEN_B}$str${Console.RESET}")

      l2 = l2 :+ str

      //c2.acknowledge(msg.getMessageId)
    }

    println()
    println(l1)
    println()

    println()
    println(l2)
    println()

    try {
      assert(l1 == l2)
    } finally {
      c1.close()
      c2.close()
      client.close()
    }
  }

}

