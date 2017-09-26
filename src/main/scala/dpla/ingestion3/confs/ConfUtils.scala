package dpla.ingestion3.confs

import java.net.URL

import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import com.amazonaws.services.s3.model.S3Object
import com.typesafe.config.Config

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

trait ConfUtils {

  /**
    * TODO: There are multiple "validateUrl" methods floating around
    * the project. Need to find them all and consolidate.
    *
    * @param url
    * @return Boolean
    */
  def validateUrl(url: String): Boolean = Try{ url match {
    case str if str.startsWith("s3") => false
    case str if str.startsWith("http") => new URL(url)
    }}.isSuccess

  /**
    * Get the contents of the configuration file
    *
    * @param path Path to configuration file
    * @return Contents of file as a String
    */
  def getConfigContents(path: String): Option[String] = {
    path match {
      case path if path.startsWith("s3") => getS3Conf(path)
      case path if path.startsWith("http") => throw new UnsupportedOperationException("HTTP not supported yet")
      case _ => getLocalConf(path)
    }
  }

  /**
    * Reads file on S3 and returns contents as Option[String]
    *
    * @param path Path to file
    * @return Contents of file as Option[String]
    */
  def getS3Conf(path: String): Option[String] = {
    val s3Client = new AmazonS3Client()
    val uri: AmazonS3URI = new AmazonS3URI(path)
    val s3Object: S3Object = s3Client.getObject(uri.getBucket, uri.getKey)
    val source: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
    // Try-block exists to ensure closure of input stream
    try
      Option(source.mkString)
    finally
      source.close()
  }

  /**
    * Reads contents of file on path
    *
    * @param path Path to file
    * @return Option[String] The contents of the file or None
    */
  def getLocalConf(path: String): Option[String] = Try {
      Source.fromFile(path).getLines.mkString
    } match {
      case Success(s) => Some(s)
      case Failure(_) => None
    }

  /**
    *
    * @param conf
    * @param prop
    * @param default
    * @return
    */
  def getProp(conf: Config, prop: String, default: Option[String] = None): Option[String] = {
    conf.hasPath(prop) match {
      case true => Some(conf.getString(prop))
      case false => default
    }
  }
}
