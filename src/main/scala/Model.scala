package neu.pdpmr.project

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object Model {
    def main(args: Array[String]): Unit ={

      val tempDirPath: Option[String] = util.Properties.envOrNone("TEMP_DIR_PATH")
      var temPath = "temp"
      if (!tempDirPath.isEmpty) temPath = tempDirPath.get

      val datasetjarpath = getClass.getResource("/dataset_820k.csv")
      val datasetfsPath = new File(temPath + "/dataset_820k.csv")
      FileUtils.copyURLToFile(datasetjarpath, datasetfsPath)

      val modeljarpath = getClass.getResource("/pretrained_model.zip")
      val modelfsPath = new File(temPath + "/pretrained_model.zip")
      FileUtils.copyURLToFile(modeljarpath, modelfsPath)

      Process("unzip -n "+ temPath +"/pretrained_model.zip -d " + temPath)!

      val queryFile = args(0)
      val datasetFile = args (1)
      val conf = new SparkConf().setMaster("local[*]")setAppName("Predicting Download")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val modelPath = "pretrained_model"
      var dataset = DataReader.getPredictionDataFrame(queryFile,"temp/dataset_820k.csv",sc,sqlContext)

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
