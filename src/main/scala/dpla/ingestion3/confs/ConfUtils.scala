package dpla.ingestion3.confs

import java.net.URL

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
      case path if path.startsWith("http") => throw new UnsupportedOperationException("HTTP not supported yet")
      case _ => getLocalConf(path)
    }
  }

  /**
    * Reads contents of file on path
    *
    * @param path Path to file
    * @return Option[String] The contents of the file or None
    */
  def getLocalConf(path: String): Option[String] = Try {
      Source.fromFile(path).getLines.mkString("\n")
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
