package dpla.ingestion3.entries

import java.lang.annotation.Target

object Entry {
  def suppressUnsafeWarnings(): Unit = classOf[Target].getModule.addOpens("java.nio", classOf[org.apache.spark.unsafe.Platform].getModule)
}
