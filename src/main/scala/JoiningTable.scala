import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Patrizio on 2017/11/27.
  */
object JoiningTable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)

    val downloadInput = sc.textFile("downloads.csv")
    val downloads = downloadInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    System.out.println("before filter download " + downloads.count())
    val downloadRecord = downloads.map(row => new DownloadInfo(row)).filter(_.checkValidity())
    System.out.println("after filter download " + downloadRecord.count())
    val songInput = sc.textFile("MillionSongSubset/song_info.csv")
    val songs = songInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    System.out.println("before filter " + songs.count())

    val songRecord = songs.map(row => new SongInfo(row))//.filter(_.checkValidity())
    System.out.println("after filter " + songRecord.count())


    val download_keyPair = downloadRecord.map(d => (d.getArtist(), d.getTitle(), d.getMeanPrice(), d.getDownload(), d.getConfidence()))
    //download_keyPair is (key,value) where key -> (artist, title); value -> (price, download, confidence)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

    val download_df = spark.createDataFrame(download_keyPair).toDF("Art_name", "title", "meanPrice", "download", "confidence")


    val songInfo_keyPair = songRecord.map(
      s => (s.getArtistName(), s.getTitle(),
        s.getTrackId(), s.getSongId(), s.getArtFam(), s.getArtHot(), s.getDuration(), s.getLoudness(), s.getSongHot(), s.getTempo()))
    //songInfo_keyPair: same key as download_keyPair

    val songInfo_df = spark.createDataFrame(songInfo_keyPair).toDF("Art", "titles", "trackId", "song_id", "ArtFam", "ArtHot", "duration", "loudness", "songHot", "tempo")

//    val joint = songInfo_df.join(download_df, songInfo_df("Art") === download_df("Art_name")
//      && songInfo_df("titles") === download_df("title"), "left_outer")

    val download_songInfo_DF = songInfo_df.join(download_df,songInfo_df("Art") === download_df("Art_name")
         && songInfo_df("titles") === download_df("title"), "inner").drop(download_df("Art_name")).drop(download_df("title"))

    val jamInput = sc.textFile("jam_to_msd.tsv")
    val jams = jamInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Jam(_))
    val jamCount = jams.map(j => (j.getTrack(),j.getJam())).countByKey()
    val jam_rdd = sc.parallelize(jamCount.toSeq)// convert Collection[Map] to RDD
    val jamDF = spark.createDataFrame(jam_rdd).toDF("JamTrackId","JamCount")// get jam count data frame
    //
    val jam_download_song_DF = download_songInfo_DF.join(jamDF,download_songInfo_DF("trackId") === jamDF("JamTrackId"),"left_outer").drop(jamDF("JamTrackId")).na.fill(0,Seq("JamCount"))

    val tasteInput = sc.textFile("train_triplets.txt")
    val taste = tasteInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Taste(_))
    val taste_keyPair = taste.map(t => (t.getSong(),t.getCount())).reduceByKey(_+_)
    val tasteDF = spark.createDataFrame(taste_keyPair).toDF("SongId","TasteCount") // get taste count data frame

    val taste_jam_download_song_DF = jam_download_song_DF.join(tasteDF, jam_download_song_DF("song_id") === tasteDF("SongId"),"left_outer").drop(tasteDF("SongId")).na.fill(0,Seq("TasteCount"))

    val rows: RDD[Row] = taste_jam_download_song_DF.rdd

    System.out.println("after joint " + rows.count())

    val result = rows.collect().toList

    val name = "out2"
    val fw = new FileWriter(name)
    for (i <- result) {
      fw.append(i.toString().substring(1, i.toString().length - 1))
      fw.append("\n")
    }
    fw.close()


  }

  //  def getTop5HottestGenre(songRecord: RDD[SongInfo], downloadRecord: RDD[DownloadInfo]): Unit = {
  //
  //    val song = songRecord.map(lines => (lines.getCombinedKey(),lines.getDuration()))
  //    val download = downloadRecord.map(lines => (lines.getCombinedKey(),lines.getDownload()))
  //    val join = song.join(download)
  //  }


}



