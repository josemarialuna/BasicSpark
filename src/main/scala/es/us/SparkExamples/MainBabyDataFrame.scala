package es.us.SparkExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Jose Maria Luna.
  */
object MainBabyDataFrame {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Baby Main")
      .master("local[*]")
      .getOrCreate()

    val dataPath = "Resources\\baby_names.csv"


    val dfRead = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(dataPath)

    dfRead.printSchema()

    //TODO: Número de nombres diferentes
    val totalNames = dfRead.select("First Name")
      .distinct()
      .count()
    println(s"Total Names: $totalNames")


    //TODO: mostrar los 50 primeros nombres de niñas
    val girlsNames = dfRead.where("Sex = 'F'")
    girlsNames.show(50)


    //TODO: Nombres de niños de Orange County a partir del 2011
    val orangeBoys = dfRead.where("County = 'ORANGE'")
      .where("Sex = 'M'")
      .where("Year >= 2011")
    orangeBoys.show()




  }

}
