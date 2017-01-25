package la.dp.ingestion3.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.Text

/**
  * Created by scott on 1/18/17.
  */
object Utils {

  /**
    *
    * @param fs
    * @param seqFilePath
    * @param conf
    * @return
    */
  def printSeqFile(fs: FileSystem, seqFilePath: Path, conf: Configuration): Unit = {
    val reader: Reader = new Reader(fs, seqFilePath, conf)


    val k: Text = new Text()
    val v: Text = new Text()

    try {
      while (reader.next(k, v)) {
        println(k);
      }
    } finally {
      reader.close()
    }
  }
}
