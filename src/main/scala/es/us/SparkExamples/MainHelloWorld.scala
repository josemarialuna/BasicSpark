package es.us.SparkExamples


import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna.
  */
object MainHelloWorld {

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Hello World Main")
      .master("local[*]")
      .getOrCreate()


    println("Hello World")

    //TODO: Crea una variable mutable y otra inmutable
    var mutable = 2
    val inmutable = 1


    //TODO: Modifica el valor de la varible mutable y de la inmutable
    mutable = 41
    //inmutable = 2

    //TODO: Crea una variable de tipo String y separe sus caracteres por espacios
    val tipoString = "Hola esto es una variable tipo string"
    val separada = tipoString.split(" ")

    //TODO: Pasa a RDD ese array
    val rddSeparada = spark.sparkContext.parallelize(separada)


    //TODO: Crea un array
    val esteArray = Array(1, 2, 3)


    //TODO: Pasa el array a String e imprimelo por pantalla
    val arrayString = esteArray.mkString("->") // "1->2->3"


    //TODO: Crea una función que sume 1.5 al valor introducido
    def sumaUno(valor: Int): Double = {
      valor + 1.5
    }

    //TODO: Llama a la función y prueba a pasarle los siguientes parámetros: un entero, un double y un string
    sumaUno(2)
    //sumaUno(2.1)
    //sumaUno("hola")

  }

}
