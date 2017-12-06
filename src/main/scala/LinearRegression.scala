import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator

object LinearRegression {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Specify dataset and model names
    val datasetName = "dataset_32k.csv"

    val modelName = "LinearRegression-" + datasetName

    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0) + "/" + datasetName)
      .distinct()
      .toDF("songId", "taste_count", "jam_count", "trackId", "price", "download", "confidence", "famil", "artHot", "dur", "loud", "songHot", "tempo")
      .cache()

    val assembler = new VectorAssembler()
      .setInputCols(Array("taste_count", "jam_count", "price", "famil", "artHot", "dur", "loud", "songHot", "tempo"))
      .setOutputCol("features")
    dataset = assembler.transform(dataset).cache()
    dataset = dataset.select("download", "features").cache()
    dataset = dataset.withColumnRenamed("download", "label").cache()


    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))


//    println("Start training models.....")
//    val lr = new LinearRegression()
//    val model = lr.fit(train)
//    model.save(args(1) + "/" + modelName)


    println("Loading pre-trained model")
    val model =  LinearRegressionModel.load(args(1)+"/"+modelName)

    println("Making predictions")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Dataset size " + dataset.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  }



}
