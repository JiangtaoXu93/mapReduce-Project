package neu.pdpmr.project

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object Model {
    def main(args: Array[String]): Unit ={

      val queryFile = args(0)
      val datasetFile = args (1)
      val conf = new SparkConf().setMaster("local[*]")setAppName("Predicting Download")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val modelPath = "pretrained_model"
      var dataset = DataReader.getPredictionDataFrame(queryFile,datasetFile,sc,sqlContext)

      println("Loading pre-trained model")
      val trained_model = GBTRegressionModel.load(modelPath)
      println("Making predictions")
      val predictions = trained_model.transform(dataset)

      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      println("Dataset size "+dataset.count())
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)

      val outdir = "temp"
      // output predictions to csv
      predictions.select("prediction").coalesce(1).write.mode("overwrite").csv(outdir)

    }



}
