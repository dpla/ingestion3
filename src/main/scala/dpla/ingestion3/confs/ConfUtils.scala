package dpla.ingestion3.confs

import com.typesafe.config.Config

import java.net.URL
import scala.io.Source
import scala.util.{Try, Using}

trait ConfUtils {

  /** TODO: There are multiple "validateUrl" methods floating around the
    * project. Need to find them all and consolidate.
    */
  def validateUrl(url: String): Boolean = Try {
    url match {
      case str if str.startsWith("s3")   => false
      case str if str.startsWith("http") => new URL(url)
    }
  }.isSuccess

  /** Get the contents of the configuration file
    *
    * @param path
    *   Path to configuration file
    * @return
    *   Contents of file as a String
    */
  def getConfigContents(path: String): Option[String] = {
    path match {
      case path if path.startsWith("http") =>
        throw new UnsupportedOperationException("HTTP not supported yet")
      case _ => getLocalConf(path)
    }
  }

  /** Reads contents of file on path
    *
    * @param path
    *   Path to file
    * @return
    *   Option[String] The contents of the file or None
    */
  private def getLocalConf(path: String): Option[String] =
    Using(Source.fromFile(path)) { source =>
      source.getLines.mkString("\n")
    }.toOption

  /** @param conf
    * @param prop
    * @param default
    * @return
    */
  def getProp(
      conf: Config,
      prop: String,
      default: Option[String] = None
  ): Option[String] =
    if (conf.hasPath(prop)) {
      Some(conf.getString(prop))
    } else {
      default
    }
}
