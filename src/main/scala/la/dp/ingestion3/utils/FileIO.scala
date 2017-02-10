package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecordBuilder}

/**
  * Basic FileIO ops
  *
  */
class FlatFileIO extends FileIO {
  /**
    * Save the file to disk
    *
    * @param record String
    * @param outputFile String
    */
  def writeFile(record: String, outputFile: String): Unit = {
    val outFile = new File(outputFile).toPath
    Files.write(outFile, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }
}

/**
  * Write RDD to Avro file
  *
  * @param schema Schema
  *                 The schema to serialize to
  * @param outputFile File
  *                   The Avro file destination
  */
class AvroFileIO (schema: Schema, outputFile: File) extends FileIO {

  // Create and configure the writer
  val writer = new DataFileWriter[Object](new GenericDatumWriter[Object]())
  writer.setCodec(CodecFactory.snappyCodec())
  writer.create(schema, outputFile)
  val builder = new GenericRecordBuilder(schema)

  /**
    * Saves data to AvroFile
    *
    * @param id String
    *           DPLA identifier
    * @param data String
    *             Record content
    */
  override def writeFile(id: String, data: String): Unit = {
    // TODO: QUESTION >  is this the right place to do this?
    builder.set("or_document", data)
    builder.set("or_mimetype", "application/xml")
    builder.set("id", id)

    writer.append(builder.build())
  }

  /**
    * Close the writer
    *
    */
  def close: Unit = {
    writer.close()
  }
}

/**
  *
  */
trait FileIO {
  def writeFile(record: String, outputFile: String): Unit
}