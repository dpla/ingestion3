package dpla.ingestion3.harvesters

import java.io.File

import dpla.ingestion3.utils.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord

object AvroHelper {

  def avroWriter(
      nickname: String,
      outputPath: String,
      schema: Schema
  ): DataFileWriter[GenericRecord] = {
    val filename = s"${nickname}_${System.currentTimeMillis()}.avro"
    val path = if (outputPath.endsWith("/")) outputPath else outputPath + "/"
    val outputDir = new File(path)
    outputDir.mkdirs()
    if (!outputDir.exists)
      throw new RuntimeException(s"Output directory ${path} does not exist")

    val avroWriter = AvroUtils.getAvroWriter(new File(path + filename), schema)
    avroWriter.setFlushOnEveryBlock(true)
    avroWriter
  }

}
