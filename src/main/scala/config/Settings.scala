package config

import com.typesafe.config.ConfigFactory

/**
  * Created by rramwal on 19/10/18.
  * This is a singleton class and scala guarantees that only single instance exists
  *
  */
object Settings {
  ///load all config objects
  private val config = ConfigFactory.load()


  object  WebLogGen {

    private  val webLogGen = config.getConfig("clickstream")
    ///this helps in loading only clickstream section from conf file.

    //lazy val helps in evaluating value when its used.

    lazy val records = webLogGen.getInt("records")
    lazy val timeMultiplier = webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filePath = webLogGen.getString("file_path")
    lazy val destPath = webLogGen.getString("dest_path")
    lazy val numberOfFiles = webLogGen.getInt("number_of_files")


  }



}
