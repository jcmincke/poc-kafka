package V3

import java.util
import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer

import org.apache.kafka.streams.StreamsConfig
import org.joda.time.{DateTime, DateTimeZone}

import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat


object Server  {

  val host = "0.0.0.0"

  implicit val system: ActorSystem = ActorSystem("Main")
  implicit val materializer : ActorMaterializer = ActorMaterializer()

   def main(args: Array[String]) {

     Config.parseConfig(args) match {
       case Some(config) => {
         println(config)
         start(config)
       }
       case _ => println("Bad Command Line")
     }

    def start(config: Config) {

      // Topics.
      val msgTopic = "message"
      val aggregatedMsgTopic = "aggregatedMessage"


      // Set up the producer.
      val producerProps = new Properties()
      producerProps.put("bootstrap.servers", "localhost:9092")

      val producer = new Producer(producerProps)


      // Setup stream processor (aggregator)

      val aggregatorProps = new util.HashMap[String, Object]()
      aggregatorProps.put(StreamsConfig.APPLICATION_ID_CONFIG, config.app)
      aggregatorProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      val aggregatorStreamConfig = new StreamsConfig(aggregatorProps)

      // Start the aggregator and get the store that contains the aggregated messages.
      val store = Aggregator.startAggregator(aggregatorStreamConfig, msgTopic, aggregatedMsgTopic)

      // routes definition.
      lazy val routes: Route =
        pathPrefix("analytics") {
          post {
            // parameters()
            parameters('timestamp.as[Long], 'user.as[String], 'event.as[String]) { (timestamp, user, event) => {
              for {
                ts <- checkTimestamp(timestamp)
                evt <- decodeEvent(event)
                _ = producer.sendMsg(msgTopic, ts, user, evt)
              } yield ()

              complete(StatusCodes.NoContent)
            }

            }
          }
        } ~
          pathPrefix("analytics") {
            get {
              // parameters()
              parameters('timestamp.as[Long]) { (timestamp) => {
                val amsg = store.get(Misc.getHour(new DateTime(timestamp, DateTimeZone.UTC)).getMillis())
                val r = if (amsg != null) AnalyticsResult(amsg.users.size, amsg.nbClicks, amsg.nbImpressions)
                else AnalyticsResult(0, 0, 0)
                complete(r)
              }
              }
            }
          }

      Http().bindAndHandle(routes, host, config.port)
      println("Ready")
    }
  }


  /** Check timestamp validity. A timestamp should be positive.
    */
  def checkTimestamp(timestamp : Long) : Option[Long] = if (timestamp > 0) Some(timestamp) else None

  /** Decode the {click|impression} event
    */
  def decodeEvent(event : String) : Option[Event] =
    event match {
      case "click" => Some (Click())
      case "impression" => Some (Impression())
      case _ => None
    }
}


// result returned by the GET query
case class AnalyticsResult(nbUsers : Int, nbClicks : Int, nbImpressions : Int)

object AnalyticsResult {
  implicit val analyticsResultJsonFormat: RootJsonFormat[AnalyticsResult] = jsonFormat3(AnalyticsResult.apply)
}