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


  def getEvaluationDataFrame(queryFile: String, dataSetFile: String, sc: SparkContext): DataFrame = {
    // return should be Dataframe
    // input file format : a txt file with first column artist , second column song tittle
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate
    val dataset = Source.fromFile(dataSetFile)
    val datasetIter = dataset.getLines().map(_.split(","))
    val query = Source.fromFile(queryFile)
    val queryIter = query.getLines().map(_.split(";"))
    val result = new StringBuilder
    // print the uid for Guest
    for (l <- queryIter) {
      val compound_key = l(0).toLowerCase.replaceAll("\\s", "").replace(",", "").replace(";", "") + "_" +
        l(1).toLowerCase.replaceAll("\\s", "").replace(",", "").replace(";", "")
      for (i <- datasetIter) {
        val key = i.toString.split(",")(0)
        if (i(0).equals(compound_key)) {
          result.append(i)
          result.append(";")
        }
      }
      val test = 1
    }

    query.close()
    dataset.close()

    val name = "selected_record"
    val fw = new FileWriter(name)
    val temp = result.toString().split(";")
    for (i <- temp) {
      fw.append(i.toString)
      fw.append("\n")
    }
    fw.close()

    val songInput = sc.textFile("selected_record")

    val songInfos = songInput.map(s => (s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10)))
    val songInfoDF = spark_session.createDataFrame(songInfos).toDF("ArtFam", "ArtHot", "Duration", "Loudness", "SongHot", "Tempo", "meanPrice", "download", "jamCount", "tastecount")
    songInfoDF
    // feature for 680K CSV is Array("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
  }

}
