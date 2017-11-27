/**
  * Created by Patrizio on 2017/11/27.
  */
class DownloadInfo(row:String) extends java.io.Serializable {

  val line: Array[String] = row.split(";")
  var artist = ""
  var title = ""
  var meanPrice = 0.0
  var download = 0
  var confidence = ""
  var combinedKey = ""
  var isValid = true

  try {
    artist = line(0)
    title = line(1)
    combinedKey = artist+"_"+title
    meanPrice = line(2).toDouble
    download = line(3).toInt
  } catch {
    case e: Exception => isValid = false
  }

  def getArtist(): String = artist

  def getTitle(): String = title

  def getMeanPrice(): Double = meanPrice

  def getDownload(): Int = download

  def getConfidence(): String = confidence

  def getCombinedKey(): String = combinedKey

  def checkValidity():Boolean ={

    var result = false
    if (isValid && getMeanPrice() > 0.00
      && getDownload()>0
      && !getTitle().isEmpty && !getArtist().isEmpty && !getConfidence().isEmpty) {
      result = true
    }
    result

  }


}

