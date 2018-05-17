import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna on 11/05/2018.
  */
object MainTemplate {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Template Main")
      .master("local[*]")
      .getOrCreate()


    //TODO: Your code


  }

}
