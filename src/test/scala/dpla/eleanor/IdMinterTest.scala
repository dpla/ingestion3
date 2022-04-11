package dpla.eleanor

import dpla.eleanor.mappers._
import org.scalatest.FlatSpec

class IdMinterTest extends FlatSpec with IdMinter {

  "IdMinter" should "shorten Feedbooks provider name" in {
    val expected = "feedbooks"
    assert(shortenName(FeedbooksMapping.providerName) === expected)
  }

  it should "shorten Gutenberg provider name" in {
    val expected = "project-gutenberg"
    assert(shortenName(GutenbergMapping.providerName) === expected)
  }

  it should "shorten Standard Ebooks provider name" in {
    val expected = "standard-ebooks"
    assert(shortenName(StandardEbooksMapping.providerName) === expected)
  }

  it should "shorten Unglueit provider name" in {
    val expected = "unglue.it"
    assert(shortenName(UnglueItMapping.providerName) === expected)
  }

  it should "mint the correct ID for Feedbooks" in {
    // feedbooks--https://www.feedbooks.com/book/7256
    val id = "https://www.feedbooks.com/book/7256"
    assert(mintDplaId(id, FeedbooksMapping.providerName) === "eb0d9586e780358ce4dfea242229ef2f")
  }

  it should "mint the correct ID for Standard Ebooks" in {
    val id = "https://standardebooks.org/ebooks/rolf-boldrewood/robbery-under-arms"
    val expected = "fa10a7a586b209a62bfdf9999b52c2ce"
    assert(mintDplaId(id, StandardEbooksMapping.providerName) === expected)
  }

  it should "mint the correct ID for Project Gutenberg" in {
    // prehashid == project-gutenberg--ebooks/1
    val expected = "11587f8f7af0dab246d82621321ed89d"
    val id = "ebooks/1"
    assert(mintDplaId(id, GutenbergMapping.providerName) === expected)
  }

  it should "mint the correct ID for Unglueit" in {
    // unglue.it--https://unglue.it/api/id/work/198516
    val id = "https://unglue.it/api/id/work/198516"
    val expected = "d6e9a05c3ad508f35b1ac6646dafa62e"
    assert(mintDplaId(id, UnglueItMapping.providerName) === expected)
  }

  it should "throw an error when ID is empty" in {
    val id = ""
    val thrown = intercept[RuntimeException] { mintDplaId(id, UnglueItMapping.providerName) }
    assert(thrown.getMessage == "ID is empty")
  }
}
