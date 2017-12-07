import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, RandomForestRegressionModel}
import org.apache.spark.sql.SQLContext
import java.io._

object Prediction {
    def main(args: Array[String]): Unit ={

      val conf = new SparkConf().setMaster("local[*]")setAppName("Predicting Download")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      //val modelPath="model_saved/GBTRegression-dataset_820k.csv-maxIter10-maxDepth10"
      //val modelPath = "model_saved/GBTRegression-offJam-offTastedataset_820k.csv-maxIter10-maxDepth10"
      val modelPath = "model_saved/GBTRegression-offJam-offTastedataset_820k.csv-maxIter20-maxDepth15"
      //val modelPath = "model_saved/RandomForest-offJam-offTastedataset_820k.csv-numTree100-maxDepth15"
      var dataset = DataReader.getEvaluationDataFrame("data/query.txt","data/dataset_820k.csv",sc,sqlContext)

      dataset= dataset.sample(true,0.05)
      println("Loading pre-trained model")
      val trained_model = GBTRegressionModel.load(modelPath)
      //val trained_model = RandomForestRegressionModel.load(modelPath)
      println("Making predictions")
      val predictions = trained_model.transform(dataset)



      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      println("Dataset size "+dataset.count())
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)


      // output predictions to csv
      predictions
        .select("prediction")
        .coalesce(1)
        .write
        .mode("overwrite")
        .save("data/out")



    }



}
