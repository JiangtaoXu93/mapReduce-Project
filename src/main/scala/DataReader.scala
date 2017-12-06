import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature.VectorAssembler

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


  def getEvaluationDataFrame(inputFile:String, features:Array[String]): Unit ={
    // return should be Dataframe
    // input file format : a txt file with first column artist , second column song tittle


    // feature for 680K CSV is Array("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")


  }
}
