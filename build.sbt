name := "pulsar-demo"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.apache.pulsar" % "pulsar-client" % "2.8.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.apache.pulsar" % "pulsar-client-admin" % "2.8.0",

  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)