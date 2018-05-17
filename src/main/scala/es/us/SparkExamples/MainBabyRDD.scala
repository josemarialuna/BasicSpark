package es.us.SparkExamples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna.
  */
object MainBabyRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RDD Main")
      .master("local[*]")
      .getOrCreate()



    //TODO: Carga el fichero "baby_names.csv"


    //TODO: Saca por pantalla el número de filas que tiene


    //TODO: Saca por pantalla las 15 primeras filas


    //TODO: ¿Cuántos años abarca la base de datos?


    //TODO: ¿Cuántos nombres hay?


    //TODO: ¿Hay más nombres de niños o de niñas?





  }

}
