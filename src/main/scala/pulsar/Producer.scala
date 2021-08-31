package pulsar

import org.apache.pulsar.client.api.{AuthenticationFactory, ProducerAccessMode, PulsarClient}

import java.util.UUID
import java.util.concurrent.TimeUnit

object Producer {

  def main(args: Array[String]): Unit = {

    val client = PulsarClient.builder()
      .serviceUrl(SERVICE_URL)
      /*.authentication(
        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjbGllbnQ7YjBmYWRjYjUtYTg0Ny00MTBlLWFiMGItY2RjZDYzOTQ0MThiO1pHRnlkMmx1WkdJPSJ9.XSUJyWnIs3vl7nBUyOfDvtaueVlvfoIxPVZf16Ne1UxM6pvFTSw1RmuhjSK87udQ-F3kCyZSEx5nJXOCoouxE2mmKDvM6OqWYwUEGcGjmsmmx5_p-Kbz_im3AJ14jSFrjUZ1J5Ly8T3OL3vwAKC1rT3MPIwarqBwJbowNqXEESI1NBI6L6njDNFJLDNOUn7rArk6_OturuqtGPX8_vgTmawB0uonfmmTxXNodewWOxsqEkXNGkOQwJ_KwmUNAcrj7qkY7VVVTdD7H7ykK4A3ipc54o8nuFjHXt_UhgyPFFKNDoIn9r402nYqD359wdX13Qkix9RmOn517b8PfeGZ3A")
      )*/
      .allowTlsInsecureConnection(true)
      .build()

    val producer = client.newProducer()
      .topic(TOPIC)
      .enableBatching(true)
      //.accessMode(ProducerAccessMode.Exclusive)
      .create()

    for(i<-0 until 100){
      val key = UUID.randomUUID().toString.getBytes()
      //val key = s"Hello-${i}".getBytes()
      producer.newMessage().orderingKey("k0".getBytes()).value(key).send()

      println(s"produced msg: ${i.toString}")
    }

    producer.flush()

    producer.close()
    client.close()
  }

}

