name := "techtest"

version := "0.1"

scalaVersion := "2.12.8"

val kafka_streams_scala_version = "0.2.1"
val circe_version = "0.10.0"
val kafka_streams_circe_version = "0.5"

libraryDependencies ++= Seq(
    "com.lightbend" %% "kafka-streams-scala" % kafka_streams_scala_version
    , "com.goyeau" %% "kafka-streams-circe" % kafka_streams_circe_version
    , "joda-time" % "joda-time" % "2.9.9"
    , "io.circe" %% "circe-core" % circe_version
    , "io.circe" %% "circe-generic" % circe_version
    , "io.circe" %% "circe-parser" % circe_version
    , "com.typesafe.akka" %% "akka-actor" % "2.5.12"
    , "com.typesafe.akka" %% "akka-stream" % "2.5.12"
    , "com.typesafe.akka" %% "akka-http" % "10.1.3"
    , "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.3"
    , "io.spray" %%  "spray-json" % "1.3.4"
    , "com.github.scopt" %% "scopt" % "4.0.0-RC2"
)
