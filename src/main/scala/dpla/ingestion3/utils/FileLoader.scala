package dpla.ingestion3.utils

import scala.io.Source

// Get data from files
trait FileLoader {
  def files: Seq[String] // NPE if val, Order of operations? Why?
  def getVocabFromCsvFiles(files: Seq[String]): Set[Array[String]] =
    getVocabFromFiles(files).map(_.split(","))
  def getVocabFromFiles(files: Seq[String]): Set[String] =
    files.flatMap(readFile).toSet
  // Read text files ignoring lines starting with #
  def readFile(file: String): Seq[String] =
    Source.fromInputStream(getClass.getResourceAsStream(file))
        .getLines()
      .filterNot(_.startsWith("#"))
      .toSeq
}
