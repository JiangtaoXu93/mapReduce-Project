import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler

import scala.io.Source

object DataReader {

  def convertCSV(csvName: String, datasetDir: String, sqlContext: SQLContext): DataFrame = {


    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(datasetDir + "/" + csvName)
      .distinct()


    // get features and labels
    val assembler = new VectorAssembler()

    csvName match {

      case "dataset_5k.csv" | "dataset_32k.csv" => {

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
        dataset = dataset.toDF("artist", "songTitle", "trackID", "songID", "artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
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



  def convertCSV_LinearRegression(csvName: String, datasetDir: String, sqlContext: SQLContext): DataFrame = {


    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(datasetDir + "/" + csvName)
      .distinct()


    // get features and labels
    val assembler = new VectorAssembler()

    csvName match {

      case "dataset_5k.csv" | "dataset_32k.csv" => {

        dataset = dataset.toDF("songId", "taste_count", "jam_count", "trackId", "price", "download", "confidence", "famil", "artHot", "dur", "loud", "songHot", "tempo")
          .cache()

        //assembler.setInputCols(Array("taste_count", "jam_count", "price", "famil", "artHot", "dur", "loud", "songHot", "tempo"))
        assembler.setInputCols(Array("price", "famil", "artHot", "dur", "loud", "songHot", "tempo"))
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
        dataset = dataset.toDF("artist", "songTitle", "trackID", "songID", "artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
          .na.drop()

        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset)

      }
      case "dataset_800k.csv" => {
        dataset = dataset.toDF("artistAndSongTitle", "artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tastecount")
          .na.drop()

        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download"))
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
    //val conf = new SparkConf().setMaster("local").setAppName("Joint2DataFrame")

    // Create two dataframe, one for query and the other for our dataset
    val dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataSetFile)
      .distinct()

    val dataSetRecord = dataset.toDF("key", "artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tasteCount").cache()



    val queryInfo = sc.textFile(queryFile).map(q => (q.split(";")(0).toLowerCase.replaceAll("\\s","").replaceAll("\\p{P}","")+"_"
      +q.split(";")(1).toLowerCase.replaceAll("\\s","").replaceAll("\\p{P}",""),1))

    val queryRecord = spark_session.createDataFrame(queryInfo).toDF("key","count").drop("count")

    var joint = queryRecord.join(dataSetRecord,queryRecord("key")===dataSetRecord("key"),"inner").drop(dataSetRecord("key"))


    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tasteCount"))
      .setOutputCol("features")
    joint = assembler.transform(joint)
    joint = joint.select("download", "features")
    joint = joint.withColumnRenamed("download", "label").cache()
    joint


  }

}
