package V3

import java.util.Properties


import com.lightbend.kafka.scala.streams._
import com.goyeau.kafka.streams.circe.CirceSerdes

import io.circe.generic.auto._

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{Consumed, KafkaStreams}
import org.apache.kafka.streams.kstream.{Materialized, Serialized}
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.StreamsConfig

import org.joda.time.{DateTime, DateTimeZone}
import org.apache.kafka.streams.kstream.Produced


class Producer(props : Properties) {

  val producer = new KafkaProducer[Long, Msg](props, CirceSerdes.serializer[Long], CirceSerdes.serializer[Msg])

  /** Sends a message to the specified topic
    */
  def sendMsg(topic : String, timestamp : Long, user : String, event : Event) = {

    val ts = new DateTime(timestamp, DateTimeZone.UTC)

    val theHour = Misc.getHour(ts)

    // We include the hour in the message to facilitate debugging (via Kafka console consumer)
    val msg : Msg =  Msg(theHour.getMillis(), timestamp, user, event)

    // Here, the key is the timestamp's hour because aggregation is done by hour.
    val record = new ProducerRecord(topic, theHour.getMillis(), msg)

    producer.send(record)
  }
}



object Aggregator {

  /** Starts the aggregator consumer.
    *
    * @param streamConfig : stream configuration.
    * @param msgTopic: topic containing message to aggregate.
    * @param aggregatedMsgTopic: out topic containing aggregated messages.
    * @return: a Store that contains aggregated messages, indexed by hours.
    */
  def startAggregator(streamConfig : StreamsConfig, msgTopic : String, aggregatedMsgTopic : String) : ReadOnlyKeyValueStore[Long, AggregatedMsg] = {

    implicit val consumer = Consumed.`with`(CirceSerdes.serde[Long], CirceSerdes.serde[Msg])
    implicit val produced = Produced.`with`(CirceSerdes.serde[Long], CirceSerdes.serde[AggregatedMsg])
    implicit val serialized : org.apache.kafka.streams.kstream.Serialized[Long, Msg] = Serialized.`with`(CirceSerdes.serde[Long], CirceSerdes.serde[Msg])

    implicit val m = Materialized.as[Long, AggregatedMsg, KeyValueStore[Bytes, Array[Byte]]]("store")

    // We could use binary serdes, but that's an MVP
    m.withKeySerde(CirceSerdes.serde[Long])
    m.withValueSerde(CirceSerdes.serde[AggregatedMsg])

    val builder = new StreamsBuilderS

    val msgStream : KStreamS[Long, Msg] = builder.stream[Long, Msg](msgTopic)

    val aggregatedMsgStream : KTableS[Long, AggregatedMsg] =
      msgStream.groupBy[Long]((k, _) => k)
      .aggregate(() => AggregatedMsg.default, (h:Long, msg:Msg, acc) => {
          val (nbClicks, nbImpressions) = msg.event match {
            case Click() => (acc.nbClicks+1, acc.nbImpressions)
            case Impression() => (acc.nbClicks, acc.nbImpressions+1)
            }
          val r = AggregatedMsg(h, acc.users+msg.user, nbClicks, nbImpressions)
          println(r)
          r
    }, m)


    // We send the aggregated message to an out topic.
    // Not strictly necessary but helpful if we want to inspect the aggregated messages with the kafka console consumer
    aggregatedMsgStream.toStream.to(aggregatedMsgTopic)

    val stream = new KafkaStreams(builder.build, streamConfig)

    stream.start()

    // wait until the stream is started before retrieving the store.
    Thread.sleep(10000)

    val store : ReadOnlyKeyValueStore[Long, AggregatedMsg] = stream.store("store", QueryableStoreTypes.keyValueStore[Long, AggregatedMsg])

    store
  }
}


