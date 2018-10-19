package BatchJob

import config.Settings
import domain.Activity
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rramwal on 19/10/18.
  */
object BatchJobDF {
  def main(args: Array[String]): Unit = {
    //get spark config

    val conf = new SparkConf().setMaster("local[*]").setAppName("BatchJobRDD").set("spark.driver.host", "localhost");


    //set up spark context

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //sourcing the input file

    val input = sc.textFile(Settings.WebLogGen.filePath)


    val inputDF = input.flatMap { line =>
      val record = line.split("\\t")
      if (record.length == 7)
        Some(Activity(record(0), record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    val df = inputDF.select(
      inputDF("timestamp_hour"), inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("visitor"), inputDF("page"), inputDF("product")
    ).cache()

    //register a temp table

    df.registerTempTable("WebActivity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors from
        |WebActivity GROUP BY product, timestamp_hour
      """.stripMargin)

    //  visitorsByProduct.foreach(println)
    visitorsByProduct.write.partitionBy("product").mode(SaveMode.Append).json(Settings.WebLogGen.destPath)

    val visitorsPurchasedProduct = sqlContext.sql(
      """select product, timestamp_hour, action , visitor from
        |WebActivity where action ="purchase" group by  action,visitor,product,timestamp_hour
      """.stripMargin).cache()


    visitorsPurchasedProduct.write.mode(SaveMode.Append).json(Settings.WebLogGen.destPath)

  }

}
