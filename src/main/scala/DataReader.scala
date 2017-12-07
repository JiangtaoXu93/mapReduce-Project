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


  def getEvaluationDataFrame(inputFile:String, features:Array[String], sc:SparkContext): DataFrame ={
    // return should be Dataframe
    // input file format : a txt file with first column artist , second column song tittle
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate
    val src = Source.fromFile(inputFile)
    val iter = src.getLines().map(_.split(":"))
    val result = new StringBuilder
    // print the uid for Guest
    for (l <- iter){
      val compound_key = l(0).toLowerCase.replaceAll("\\s", "").replace(",", "").replace(";", "")+"_"+
        l(1).toLowerCase.replaceAll("\\s", "").replace(",", "").replace(";", "")
      for (i <- features.length){
        if(features(i)(0).equals(compound_key)){
          result.append(features(i))
          result.append(";")
        }
      }
    }

    // the rest of iter is not processed
    src.close()

    val name = "selected_record"
    val fw = new FileWriter(name)
    val temp = result.toString().split(";")
    for (i <- temp) {
      fw.append(i.toString)
      fw.append("\n")
    }
    fw.close()

    val songInput = sc.textFile("selected_record")

    val songInfos = songInput.map(
      s => (s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11)))
    val songInfoDF = spark_session.createDataFrame(songInfos).toDF("ArtFam","ArtHot","Duration","Loudness","SongHot","Tempo","meanPrice","download","confidence","jamCount","tastecount")
    songInfoDF
    // feature for 680K CSV is Array("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
  }
}