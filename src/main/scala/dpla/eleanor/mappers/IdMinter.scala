package dpla.eleanor.mappers

import org.apache.commons.codec.digest.DigestUtils

trait IdMinter {

  /**
    *
    * @param providerName
    * @return
    */
  def shortenName(providerName: String): String =
    providerName
      .toLowerCase()
      .replaceAll("( )+", "-")

  /**
    *
    * @param id
    * @param providerName
    * @return
    */
  def mintDplaId(id: String, providerName: String): String = {
    (id.isEmpty, providerName.isEmpty) match {
      case (true, true) => throw new RuntimeException("Provider name and ID are empty")
      case (true, false) => throw new RuntimeException("ID is empty")
      case (false, true) => throw new RuntimeException("Provider name is empty")
      case (_, _) => DigestUtils.md5Hex(s"${shortenName(providerName)}--$id")
    }
  }

}
