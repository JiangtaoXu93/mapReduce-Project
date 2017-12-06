import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object getCleanedSongInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate

    val songInput = sc.textFile("all/song_info.csv")
    val songs = songInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    val songRecord = songs.map(row => new NewSongInfo(row))//read song Info from files
    val artist_songInfo_keyPair = songRecord.map(s => (s.getArtId(),
      (s.getTempo(),s.getSongHot(),s.getArtHot(),s.getLoudness(),s.getDuration(),s.getArtFam()))).groupByKey()
//
//    val name = "avarage_song_info"
//    val pw = new PrintWriter(new File(name))
//
//    for(artist_iter <- artist_songInfo_keyPair.collect().toList) {
//      var tempoSum = 0.0;
//      var songHotSum = 0.0;
//      var artHotSum = 0.0;
//      var loudSum = 0.0;
//      var durationSum = 0.0;
//      var artFamSum = 0.0;
//
//      var tempoCount = 0;
//      var songHotCount = 0;
//      var artHotCount = 0;
//      var loudCount = 0;
//      var durationCount = 0;
//      var artFamCount = 0;
//
//      var iters = artist_iter._2
//      iters.foreach(x => {
//        if (Try(x._1.toDouble).isSuccess) {
//          tempoSum = tempoSum + x._1.toDouble
//          tempoCount = tempoCount + 1
//        }
//
//        if (Try(x._2.toDouble).isSuccess) {
//          songHotSum = songHotSum + x._2.toDouble
//          songHotCount = songHotCount + 1
//        }
//
//        if (Try(x._3.toDouble).isSuccess) {
//          artHotSum = artHotSum + x._3.toDouble
//          artHotCount = artHotCount + 1
//        }
//
//        if (Try(x._4.toDouble).isSuccess) {
//          loudSum = loudSum + x._4.toDouble
//          loudCount = loudCount + 1
//        }
//
//        if (Try(x._5.toDouble).isSuccess) {
//          durationSum = durationSum + x._5.toDouble
//          durationCount = durationCount + 1
//        }
//
//        if (Try(x._6.toDouble).isSuccess) {
//          artFamSum = artFamSum + x._6.toDouble
//          artFamCount = artFamCount + 1
//        }
//      })
//
//
//      if (tempoCount == 0) tempoCount = 1
//      if (songHotCount == 0) {
//        songHotCount = artHotCount
//        songHotSum = artHotSum
//      }
//      if (loudCount == 0) loudCount = 1
//      if (durationCount == 0) durationCount = 1
//      if (artFamCount == 0) {
//        artFamCount = 1
//        artFamSum = 0.5
//      }
//
//      if (artHotCount != 0) {
//        pw.println(artist_iter._1 + ","
//          + (tempoSum / tempoCount).toString() + ","
//          + (songHotSum / songHotCount).toString() + ","
//          + (artHotSum / artHotCount).toString() + ","
//          + (loudSum / loudCount).toString() + ","
//          + (durationSum / durationCount).toString() + ","
//          + (artFamSum / artFamCount).toString())
//      }
//    }
//
//    pw.close()

    /////create a new CSV, which merge previous songInfo and average songInfo by artist

    val songInfos = songRecord.map(
      s => (s.getArtId(),s.getArtistName(),s.getTitle(),s.getTrackId(), s.getSongId(),s.getArtFam(),s.getArtHot(),s.getDuration(),s.getLoudness(),s.getSongHot(),s.getTempo()))
    val songInfoDF = spark_session.createDataFrame(songInfos).toDF("ArtistId","ArtistName","Title","TrackId", "SongId","ArtFam","ArtHot","Duration","Loudness","SongHot","Tempo")// get song info data frame

    val averageSongInput = sc.textFile("avarage_song_info")
    val averageSongInfo = averageSongInput.map(row => new AverageSongInfo(row)).map(as =>
      (as.getArtistId(),as.getTempo(),as.getSongHot(),as.getArtHot(),as.getLoudness(),as.getDuration(),as.getArtFam()))
    val averageSongInfoDF = spark_session.createDataFrame(averageSongInfo).toDF("avgArtistId","avgTempo","avgSongHot","avgArtHot","avgLoudness","avgDuration","avgArtFam")// get avg song info data frame

    val mergedSongInfoDF = songInfoDF.join(averageSongInfoDF,songInfoDF("ArtistId") === averageSongInfoDF("avgArtistId"),"cross").drop(averageSongInfoDF("avgArtistId"))

    val result = mergedSongInfoDF.collect().toList

    val ouptputName = "mergedSongInfo"
    val fw = new FileWriter(ouptputName)
    for (i <- result) {
      fw.append(i.toString().substring(1,i.toString().length - 1 ))
      fw.append("\n")
    }
    fw.close()




  }

}
