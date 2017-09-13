package dpla.ingestion3.confs

import java.net.URL
import com.typesafe.config.Config
import scala.util.Try

trait ConfUtils {

  /**
    *
    * @param string
    * @return
    */
  def validateUrl(string: String): Boolean = Try{ new URL(string) }.isSuccess

  /**
    *
    * @param filePath
    */
  def getConfFileLocation(filePath: String): String = validateUrl(filePath) match {
    case true => "config.url"
    case false => "config.file"
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
