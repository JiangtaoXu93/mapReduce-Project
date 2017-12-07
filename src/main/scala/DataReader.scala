import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler

import scala.io.Source

object DataReader {

  def convertCSV(csvName: String, datasetDir: String, sqlContext: SQLContext): DataFrame = {

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)

    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(datasetDir + "/" + csvName)
      .distinct()


    // get features and labels
    val assembler = new VectorAssembler()

    csvName match {

      case "dataset_5k.csv"|"dataset_32k.csv" => {

        dataset = dataset.toDF("songId", "taste_count", "jam_count", "trackId", "price", "download", "confidence", "famil", "artHot", "dur", "loud", "songHot", "tempo")
          .cache()

        assembler.setInputCols(Array("taste_count", "jam_count", "price", "famil", "artHot", "dur", "loud", "songHot", "tempo"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset)

      }


      case "dataset_400k.csv" => {
        dataset = dataset.toDF("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
          .na.drop()


        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tastecount"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset)

      }
      case "dataset_680k.csv" => {
        dataset = dataset.toDF("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
          .na.drop()

        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tastecount"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset)

      }

    }

    dataset = dataset.select("download", "features")
    dataset = dataset.withColumnRenamed("download", "label").cache()
    dataset
  }


  def getEvaluationDataFrame(queryFile:String, dataSetFile:String, sc:SparkContext,sqlContext: SQLContext): DataFrame ={
    // return should be Dataframe
    // input file format : a txt file with first column artist , second column song tittle
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate
    val conf = new SparkConf().setMaster("local").setAppName("Joint2DataFrame")
    val sc = new SparkContext(conf)

    // Create two dataframe, one for query and the other for our dataset
    val dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/" + dataSetFile)
      .distinct()

    val dataSetRecord = dataset.toDF("key", "artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tasteCount").cache()


    val queryInfo = sc.textFile("data/" + queryFile).map(q => (q.split(";")(0).toLowerCase.replaceAll("\\s","").replaceAll("\\p{P}","")+"_"
      +q.split(";")(1).toLowerCase.replaceAll("\\s","").replaceAll("\\p{P}",""),1))

    val queryRecord = spark_session.createDataFrame(queryInfo).toDF("key","count").drop("count")

    val joint = queryRecord.join(dataSetRecord,queryRecord("key")===dataSetRecord("key"),"inner").drop(dataSetRecord("key"))

    joint
    // feature for 680K CSV is Array("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
  }
}
