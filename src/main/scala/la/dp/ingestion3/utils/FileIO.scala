package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

/**
  * Basic FileIO ops
  *
  */
class FlatFileIO extends FileIO {
  /**
    * Save the file to disk
    */
  def writeFile(record: String, outFile: File): Unit = {
    Files.write(outFile.toPath, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }

  /**
    * Accepts a Map of destination files and their contents and
    * writes them to disk
    *
    * @param records Map[File, String]
    */
  def writeFiles(records: Map[File, String]): Unit = {
    records.foreach{ case(file: File, data: String) => writeFile(data, file) }
  }
}

/**
  * FileIO implementation using SequenceFiles
  *
  */
class SeqFileIO extends  FileIO {
  /**
    * Saves data to SequenceFile
    *
    * @param record data to save
    * @param outFile
    */
  def writeFile(record: String, outFile: File): Unit = {

  }

  def writeFiles(records: Map[File, String]): Unit = {

  }
}

trait FileIO {
  def writeFile(record: String, outFile: File): Unit
  def writeFiles(records: Map[File, String]): Unit
}