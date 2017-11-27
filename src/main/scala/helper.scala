import java.io.FileWriter

/**
  * Created by Patrizio on 2017/11/27.
  */
class helper {

  def makeCSV(thingsToPrint: List[(Any, Any)], fileName: String): Unit = {
    val name = fileName
    val fw = new FileWriter(name)
    for (i <- thingsToPrint) {
      fw.append(i._1.toString)
      fw.append(",")
      fw.append(i._2.toString)
      fw.append("\n")
    }
    fw.close()
  }
}
