/**
  * Created by rramwal on 19/10/18.
  */
package object domain {

  case class Activity(timestamp_hour: String,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      visitor: String,
                      page: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
}
