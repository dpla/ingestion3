package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

/**
  * Created by scott on 1/26/17.
  *
  * A basic object for doing File IO
  *
  */
object FileIO {
  /**
    * Format the XML to storage
    */
  def writeFile(record: String, outFile: File): Unit = {
    Files.write(outFile.toPath, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }
}
