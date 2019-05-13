package V3

import org.joda.time.{DateTime, DateTimeZone}

// @JsonCodec
sealed trait Event
case class Click() extends Event
case class Impression() extends Event

sealed case class Msg(theHour : Long, timestamp : Long, user : String, event : Event)

sealed case class AggregatedMsg(theHour : Long, users : Set[String], nbClicks : Int, nbImpressions : Int)

object AggregatedMsg {

  val default = AggregatedMsg(0, Set[String](), 0, 0)

}


object Misc {

  def getHour(timestamp: DateTime) : DateTime = {

    val year = timestamp.getYear()
    val month = timestamp.getMonthOfYear()
    val day = timestamp.getDayOfMonth()
    val hour = timestamp.getHourOfDay()

    new DateTime(year, month, day, hour, 0, DateTimeZone.UTC)
  }
}