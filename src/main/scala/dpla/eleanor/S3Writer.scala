package dpla.eleanor

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import dpla.eleanor.Schemata._

object S3Writer {
  private lazy val s3Bucket = "content.dp.la"
  private lazy val ebookS3Prefix = "ebooks"
  // use s3.amazonaws.com domain because of issues with CloudFront and content-disposition header
  private lazy val domain = "https://s3.amazonaws.com"
  private lazy val quoteChar = "\"" // required for string interpolation

  private lazy val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withRegion("us-east-1")
    .withForceGlobalBucketAccessEnabled(true)
    .build()

  /**
    * Write Payloads in Mapped data to s3 using the ebook DPLA identifier in the prefix and the SHA512 as the
    * object key
    *
    * @return The full S3 path to the object
    */
  def writePayloadToS3(payload: Payload, id: String): String = {
    val key = s"$ebookS3Prefix/${id(0)}/${id(1)}/${id(2)}/${id(3)}/$id/${payload.sha512}"
    val is = new ByteArrayInputStream(payload.data)
    val metadata = new ObjectMetadata
    metadata.setContentDisposition(s"attachment;filename=$quoteChar${payload.filename}$quoteChar")
    metadata.setContentType(payload.mimeType)
    metadata.setContentLength(payload.size)
    s3.putObject(s3Bucket, key, is, metadata)

    s"$domain/$s3Bucket/$key"
  }
}
