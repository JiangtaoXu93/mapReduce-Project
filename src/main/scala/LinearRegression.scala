import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.ml.feature.VectorAssembler


object LinearRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SPARK ML")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("dataset.csv")
      .distinct()
      .toDF("songId","taste_count","jam_count", "trackId", "price","download","confidence","famil","artHot","dur","loud","songHot","tempo")
      .cache()

    val assembler = new VectorAssembler()
        .setInputCols(Array("taste_count","jam_count", "price","famil","artHot","dur","loud","songHot","tempo"))
        .setOutputCol("features")
    dataset = assembler.transform(dataset).cache()

    dataset.printSchema()


    val Array(train,test) = dataset.randomSplit(Array(0.8,0.2))

    println(train.count(),test.count())


  }




}
