package es.us.SparkExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by davgutavi on 19/06/17.
  */
object MainTestRandomForest {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("random_forest_classification")
      .master("local[*]")
      .getOrCreate()


    println("Cargando tablas de resultados...")

    //TODO: Añadir la ruta de a tabla de resultados a analizar
    val resultados = spark.read.parquet("")

    println("Calculando los verdaderos positivos: Clientes CON fraude que el clasificador etiqueta CON fraude:")

    val label_1 = resultados.where("label = 1")
    val res_1 = resultados.where("label = 1 AND prediction = 1")
    val label_1_count: Float = label_1.count()
    val res_1_count = res_1.count()
    val sol_1 = res_1_count / label_1_count * 100
    println("verdaderos positivos: " + sol_1 + "%")

    println("Calculando los falsos positivos: Clientes SIN fraude que el clasificador etiqueta CON fraude:")

    val label_0 = resultados.where("label = 0")
    val res_2 = resultados.where("label = 0 AND prediction = 1")
    val label_0_count: Float = label_0.count()
    val res2_count = res_2.count()
    val sol_2 = res2_count / label_0_count * 100
    println("falsos positivos: " + sol_2 + "%")

    println("Calculando los falsos negativos: Clientes CON fraude que el clasificador etiqueta SIN fraude:")

    val res_3 = resultados.where("label = 1 AND prediction = 0")
    val res_3_count = res_3.count()
    val sol_3 = res_3_count / label_1_count * 100
    println("falsos negativos: " + sol_3 + "%")


    println("Calculando los verdaderos negativos: Clientes SIN fraude que el clasificador etiqueta SIN fraude:")

    val res_4 = resultados.where("label = 0 AND prediction = 0")
    val res_4_count = res_4.count()
    val sol_4 = res_4_count / label_0_count * 100
    println("verdaderos negativos: " + sol_4 +"%")


    println("DONE!")

    spark.sparkContext.stop()


  }


}