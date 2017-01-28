package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

/**
  * Basic FileIO ops
  *
  */
object FileIO {
  /**
    * Save the save to disk
    */
  def writeFile(record: String, outFile: File): Unit = {
    Files.write(outFile.toPath, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }
}
