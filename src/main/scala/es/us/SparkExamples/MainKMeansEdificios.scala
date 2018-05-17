package es.us.SparkExamples

import java.util.logging.{Level, Logger}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna
  */
object MainKMeansEdificios {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("KMeans Edificios Main")
      .master("local[*]")
      .getOrCreate()


    // Load and parse the data
    val data = spark.sparkContext.textFile("XXXXXXXXXXXXX")

    //TODO: Preprocesado a los tipos de datos


    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    //TODO: Ahora queremos sacar cada dato a qué cluster pertenece


//    val resultadoClustering = parsedData.zipWithIndex()
//      .map(_.swap)
//      .mapValues(clusters.predict(_))


    //Saving the clustering results

    //TODO: Guardar punto con el cluster al que pertenece


    //TODO: Guardar centroides


  }

}
