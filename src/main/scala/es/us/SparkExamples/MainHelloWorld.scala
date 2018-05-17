package es.us.SparkExamples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna.
  */
object MainHelloWorld {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Hello World Main")
      .master("local[*]")
      .getOrCreate()


    println("Hello World")

    //TODO: Crea una variable mutable y otra inmutable


    //TODO: Modifica el valor de la varible mutable y de la inmutable


    //TODO: Crea una variable de tipo String y separe sus caracteres por espacios


    //TODO: Crea un array


    //TODO: Pasa el array a String e imprimelo por pantalla


    //TODO: Crea una función que sume 1.5 al valor introducido


    //TODO: Llama a la función y prueba a pasarle los siguientes parámetros: un entero, un double y un string



  }

}
