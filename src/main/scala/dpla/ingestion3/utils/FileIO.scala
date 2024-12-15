package dpla.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.commons.io.IOUtils

import scala.io.Source

/** Basic FileIO ops
  */
class FlatFileIO {

  /** Save the file to disk
    *
    * @param record
    *   String
    * @param outputFile
    *   String
    *
    * @return
    *   String representation of the file path
    */
  def writeFile(record: String, outputFile: String): String = {
    val outFile = new File(outputFile).toPath
    Files
      .write(outFile, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
      .toString
  }

  /** Reads a file and returns it as a single string
    */
  def readFileAsString(name: String): String = {
    val stream = getClass.getResourceAsStream(name)
    val result = Source.fromInputStream(stream).mkString
    IOUtils.closeQuietly(stream)
    result
  }

  /** Reads a file and returns a Seq[String]
    */
  def readFileAsSeq(name: String): Seq[String] = {
    // FIXME This is a lazy kludge.
    readFileAsString(name).split("\n").toSeq
  }

}

/** Helper functions for working Avros
  */
object AvroUtils {

  /** Builds a writer for saving Avros.
    *
    * @param outputFile
    *   Place to save Avro
    * @param schema
    *   Parsed schema of the output
    * @return
    *   DataFileWriter for writing Avros in the given schema
    */
  def getAvroWriter(
      outputFile: File,
      schema: Schema
  ): DataFileWriter[GenericRecord] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.setCodec(CodecFactory.deflateCodec(1))
    dataFileWriter.setSyncInterval(1024 * 100) // 100k
    dataFileWriter.create(schema, outputFile)
  }
}