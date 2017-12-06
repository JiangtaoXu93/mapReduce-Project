import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature.VectorAssembler

object CSVConverter {

  def prepareDataset(csvName: String, datasetDir: String, sqlContext: SQLContext): DataFrame = {

    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(datasetDir + "/" + csvName)
      .distinct()
      .cache()

    // get features and labels
    val assembler = new VectorAssembler()
    val
    csvName match {

      case "dataset_5k.csv"|"dataset_32k.csv" => {

        dataset = dataset.toDF("songId", "taste_count", "jam_count", "trackId", "price", "download", "confidence", "famil", "artHot", "dur", "loud", "songHot", "tempo")
          .cache()

        assembler.setInputCols(Array("taste_count", "jam_count", "price", "famil", "artHot", "dur", "loud", "songHot", "tempo"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset).cache()

      }


      case "dataset_400k.csv" => {
        dataset = dataset.toDF("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
          .na.drop()
          .cache()

        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tastecount"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset).cache()

      }
      case "dataset_680k.csv" => {
        dataset = dataset.toDF("artist","songTitle","trackID","songID","artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "confidence", "jamCount", "tastecount")
          .cache()

        assembler.setInputCols(Array("artFam", "artHot", "duration", "loudness", "songHot", "tempo", "meanPrice", "download", "jamCount", "tastecount"))
          .setOutputCol("features")
        dataset = assembler.transform(dataset).cache()

      }

    }


    dataset = dataset.select("download", "features").cache()
    dataset = dataset.withColumnRenamed("download", "label").cache()
    dataset
  }
}
