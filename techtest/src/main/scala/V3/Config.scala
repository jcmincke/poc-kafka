package V3

import scopt.OParser


case class Config 
  ( port: Int = 9000
  , app: String = "app-1"
  )

object Config {

  def parseConfig(args: Array[String]) : Option[Config] = {
    val builder = OParser.builder[Config]

    val cfgParser = {
      import builder._
      OParser.sequence(
        programName("Narrative"),
        head("Narrative", "0.1"),
        // option -p, --port
        opt[Int]('p', "port").action((p, c) => c.copy(port = p)).text("Port"),
        // option -a, --app
        opt[String]('a', "app").action((a, c) => c.copy(app = a)).text("Consumer Group"),
      )
    }

    OParser.parse(cfgParser, args, Config())
  }
}

