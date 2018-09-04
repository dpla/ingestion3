package dpla.ingestion3.utils

import org.joda.time.DateTime

class S3Helper {
  // Datetime will be set when instance is initialized.
  val dateTime = DateTime.now
  val timestamp = dateTime("yyyyMMdd_HHmmss")


}
