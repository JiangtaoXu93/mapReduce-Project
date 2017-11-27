import java.io.FileWriter

import org.apache.spark.rdd.RDD
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
    val downloadRecord = downloads.map(row => new DownloadInfo(row)).filter(_.checkValidity())

    val songInput = sc.textFile("all/song_info.csv")
    val songs = songInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    val songRecord = songs.map(row => new SongInfo(row)).filter(_.checkValidity())


  }

  def getTop5HottestGenre(songRecord: RDD[SongInfo], downloadRecord: RDD[DownloadInfo]): Unit = {

    val song = songRecord.map(lines => (lines.getCombinedKey(),lines.getDuration()))
    val download = downloadRecord.map(lines => (lines.getCombinedKey(),lines.getDownload()))
    val join = song.join(download)
  }

}



