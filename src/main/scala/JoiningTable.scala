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
    System.out.println("before filter download " + downloads.count())
    val downloadRecord = downloads.map(row => new DownloadInfo(row)).filter(_.checkValidity())
    System.out.println("after filter download " + downloadRecord.count())
    val songInput = sc.textFile("MillionSongSubset/song_info.csv")
    val songs = songInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    System.out.println("before filter " + songs.count())

    val songRecord = songs.map(row => new SongInfo(row)).filter(_.checkValidity())
    System.out.println("after filter " + songRecord.count())





    val download_keyPair = downloadRecord.map(d => ((d.getArtist(),d.getTitle()), (d.getMeanPrice(),d.getDownload(),d.getConfidence())))
      //download_keyPair is (key,value) where key -> (artist, title); value -> (price, download, confidence)
    val songInfo_keyPair = songRecord.map(
      s => ((s.getArtistName(),s.getTitle()),
        (s.getTrackId(), s.getSongId(),s.getArtFam(),s.getArtHot(),s.getDuration(),s.getLoudness(),s.getSongHot(),s.getTempo())))
      //songInfo_keyPair: same key as download_keyPair
    val download_songInfo_join = download_keyPair.join(songInfo_keyPair)
    System.out.println("download_songInfo_join " + download_songInfo_join.count())


    val jamInput = sc.textFile("jam_to_msd.tsv")
    val jams = jamInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Jam(_))
    val jamCount = jams.map(j => (j.getTrack(),j.getJam())).countByKey()
    val jam_keyPair = sc.parallelize(jamCount.toSeq)// convert Collection[Map] to RDD
    val download_song_keyPair = download_songInfo_join.map{
      case (((artist:String,title:String),
      ((price:Double,download:Int,confidence: String),(trackId: String,songId:String,famil: Double,artHot: Double,dur: Double, loud: Double,songHot:Double,tempo: Double))))
    => (trackId,(songId,price,download,confidence,famil,artHot,dur,loud,songHot,tempo))}

    val jam_download_song_join = jam_keyPair.join(download_song_keyPair)

    val tasteInput = sc.textFile("train_triplets.txt")
    val taste = tasteInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Taste(_))
    val taste_keyPair = taste.map(t => (t.getSong(),t.getCount())).reduceByKey(_+_)
    val jam_download_song_keyPair = jam_download_song_join.map{
      case ((trackId: String,(jam_count: Long,(songId,price,download,confidence,famil,artHot,dur,loud,songHot,tempo ))))
      => (songId,(jam_count, trackId, price,download,confidence,famil,artHot,dur,loud,songHot,tempo))
    }

    val final_join = taste_keyPair.join(jam_download_song_keyPair).map{
      case ((songId,(taste_count,(jam_count, trackId, price,download,confidence,famil,artHot,dur,loud,songHot,tempo))))
        => (songId,taste_count,jam_count, trackId, price,download,confidence,famil,artHot,dur,loud,songHot,tempo)
    }

    val test = final_join.collect()

    val result = final_join.collect().toList

    val name = "dataset"
    val fw = new FileWriter(name)
    for (i <- result) {
      fw.append(i.toString().substring(1,i.toString().length-1))
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



