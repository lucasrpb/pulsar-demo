package pulsar

import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    import org.apache.pulsar.client.admin.PulsarAdmin
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

    val topics = admin.topics()

    //topics.createNonPartitionedTopic(s"persistent://public/default/log4")

    //topics.deletePartitionedTopic(topic)
    //admin.namespaces().createNamespace("public/darwindb")
    admin.namespaces().setRetention("public/darwindb", new RetentionPolicies(-1, -1))
    topics.createPartitionedTopic(TOPIC, 3)

    admin.close()
  }

}
