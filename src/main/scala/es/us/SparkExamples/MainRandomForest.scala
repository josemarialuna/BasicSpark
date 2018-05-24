package es.us.SparkExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession


object MainRandomForest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("random_forest_classification")
      .master("local[*]")
      .getOrCreate()

    //Guardamos el momento del inicio de la ejecución (OPCIONAL)
    val start = System.currentTimeMillis

    //Cargamos el dataset
    println("Cargando dataeet...")
    val data = spark.read.parquet("Resources/consumosElectricos")
    data.printSchema()
    data.show(5, truncate = false)

    //SPLIT, se generan los conjuntos de entrenamiento y test
    val Array(datatrain, datatest) = data.randomSplit(Array(0.7, 0.3))

    //ASSEMBLER, se seleccionan las columnas de catacterísticas y se genera una nueva con un vector que las contenga
    println("Generando vector de características....")
    val features = datatrain.drop("label").columns
    val featureAssembler = new VectorAssembler().setInputCols(features).setOutputCol("rawFeatures")

    //MODEL, se genera el modelo y se le pasan las columnas de etiqueta de clase y conjunto de caracerísticas
    println("Generando clasificador...")
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("rawFeatures")
      .setPredictionCol("prediction")

    //PIPELINE, se añaden las etapas a ejecutar
    println("Generando pipeline...")
    val pipeline = new Pipeline().setStages(Array(featureAssembler, rf))
    pipeline.fit(datatrain)

    //PARÁMETROS
    println("Ejecutando configuraciones del modelo y validaciones...")
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(5, 10)).addGrid(rf.maxDepth, Array(5, 10)).addGrid(rf.maxBins, Array(8, 16)).build()
//          .addGrid(rf.numTrees, Array(50, 100)).addGrid(rf.maxDepth, Array(10, 20)).addGrid(rf.maxBins, Array(32, 64)).build()

    //CROSS-VALIDATION, ejecuta el algoritmo de clasificación con diferentes subconjuntos de datos
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)

    println("Fit del training...")
    val cvModel = cv.fit(datatrain)

    println("Generando resultados...")
    val bestModel = cvModel.bestModel
    val predictions = bestModel.transform(datatest)
    val resultado = predictions.select("id", "prediction", "label")

    resultado.write.option("header", "true").save("Results/resultado_RF_" + System.currentTimeMillis)

    //Guardamos tiempo de ejecución y lo mostramos en formato dd,hh,mm,ss (OPCIONAL)
    val milis = System.currentTimeMillis - start
    tiempo_total(milis)

    spark.sparkContext.stop()
  }

  def tiempo_total(m: Long): Unit = {

    var x = m / 1000
    val seg = x % 60
    x = x / 60
    val min = x % 60
    x = x / 60
    val hor = x % 24
    x = x / 24
    val dia = x

    println("Tiempo de ejecución: " + dia + " d, " + hor + " h, " + min + " m, " + seg + " s")
  }


}

