import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.SQLContext

object Prediction {
    def main(args: Array[String]): Unit ={

      val conf = new SparkConf().setMaster("local[*]")setAppName("Predicting Download")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val modelPath="model_saved/GBTRegression-dataset_820k.csv-maxIter10-maxDepth10"

      val dataset = DataReader.getEvaluationDataFrame("data/query.txt","data/dataset_820k.csv",sc,sqlContext)

      println("Loading pre-trained model")
      val trained_model = GBTRegressionModel.load(modelPath)

      println("Making predictions")
      val predictions = trained_model.transform(dataset)

      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      predictions.printSchema()
      predictions.select("prediction").coalesce(1).write.csv("data/out.csv")
      println("Dataset size "+dataset.count())
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    }
}
