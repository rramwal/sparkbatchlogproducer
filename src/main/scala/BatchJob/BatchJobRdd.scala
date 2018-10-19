package BatchJob


import config.Settings
import domain.Activity
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by rramwal on 19/10/18.
  */
object BatchJobRdd {
  def main(args: Array[String]): Unit = {


    //get spark conf
    val conf = new SparkConf().setMaster("local[*]").setAppName("BatchJobRDD").set("spark.driver.host", "localhost");


    //set up spark context
    val sc = new SparkContext(conf)


    ///initializing an RDD
    val filesources = Settings.WebLogGen
    val sourceFile = filesources.filePath
    val input = sc.textFile(sourceFile)

    val inputRdd = input.flatMap { line =>
      val record = line.split("\\t")
      if (record.length == 7)
        Some(Activity(record(0), record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    //listing unique number of visits per product

    val keybyProduct = inputRdd.keyBy(r => (r.product, r.timestamp_hour)).cache()
    val visitorsByProduct = keybyProduct.mapValues(a => a.visitor).distinct().countByKey()
    visitorsByProduct.foreach(println)

    //counting unique number of visitors per product

    val visitorsCountByProduct = keybyProduct.mapValues(a=> a.visitor).distinct().count()

    println(visitorsCountByProduct)


    //finding visitors who actually purchased

    val visitorsPurchasedProduct = keybyProduct.mapValues(a =>
      a.action matches ("purchase")).distinct().countByKey()

      visitorsPurchasedProduct.foreach(println)
    //counting number of unique product purchases by users

    val visitorsCountProductPurchased = keybyProduct.mapValues(a =>
    a.action matches ("purchase")).distinct().count()

    println(visitorsCountProductPurchased)

  }
}
