package dpla.ingestion3.utils

import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


// Get data from files
trait FileLoader {
  /**
    *
    * @return
    */
  def files: Seq[String] // NPE if val, Order of operations? Why?

  /**
    *
    * @param files
    * @return
    */
  def getVocabFromCsvFiles(files: Seq[String]): Set[Array[String]] =
    getVocabFromFiles(files).map(_.split(",", 2))

  def getVocabFromJsonFiles(files: Seq[String]): Seq[(String, String)] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    files.flatMap(file => {
      val fileContentString = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().mkString
      mapper.readValue[Map[String, String]](fileContentString)
    })
  }

  /**
    *
    * @param files
    * @return
    */
  def getVocabFromFiles(files: Seq[String]): Set[String] =
    files.flatMap(readFile).toSet

  /**
    * Read text files ignoring lines starting with `#`
    */

  def readFile(file: String): Seq[String] =
    Source.fromInputStream(getClass.getResourceAsStream(file))
        .getLines()
      .filterNot(_.startsWith("#"))
      .toSeq
}
