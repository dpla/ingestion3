package dpla.ingestion3.mappers.providers

import org.scalatest.{BeforeAndAfter, FlatSpec}

class HathiThumbnailFetcherTest extends FlatSpec with BeforeAndAfter {

  val hathiIdOpt: Option[String] = Some("000443495")
  val oclcIdOpt: Option[String] = Some("13230493")
  val isbnIdOpt: Option[String] = Some("8436305477")
  val googlePrefix: Option[String] = Some("UOM")

  val response = "var _GBSBookInfo = {\"OCLC:13230493\":\n\t{\"bib_key\":\"OCLC:13230493\",\n\t\"info_url\":\"https://books.google.com/books?id=Yv-ZJZkIpgcC\\u0026source=gbs_ViewAPI\",\n\t\"preview_url\":\"https://books.google.com/books?id=Yv-ZJZkIpgcC\\u0026printsec=frontcover\\u0026source=gbs_ViewAPI\",\n\t\"thumbnail_url\":\"https://books.google.com/books/content?id=Yv-ZJZkIpgcC\\u0026printsec=frontcover\\u0026img=1\\u0026zoom=5\\u0026edge=curl\",\"preview\":\"full\",\"embeddable\":true,\"can_download_pdf\":false,\"can_download_epub\":false,\"is_pdf_drm_enabled\":false,\"is_epub_drm_enabled\":false}};"

  it should "construct correct request URL (without ISBN)" in {
    val fetcher = new HathiThumbnailFetcher(hathiIdOpt, oclcIdOpt, None, googlePrefix)
    val expected = Some("http://books.google.com/books?jscmd=viewapi&bibkeys=UOM:000443495,OCLC:13230493")
    assert(fetcher.requestUrl == expected)
  }

  it should "construct correct request URL (with ISBN)" in {
    val fetcher = new HathiThumbnailFetcher(hathiIdOpt, oclcIdOpt, isbnIdOpt, googlePrefix)
    val expected = Some("http://books.google.com/books?jscmd=viewapi&bibkeys=UOM:000443495,OCLC:13230493,ISBN:8436305477")
    assert(fetcher.requestUrl == expected)
  }

  it should "return None if hathiId is missing" in {
    val fetcher = new HathiThumbnailFetcher(None, oclcIdOpt, None, googlePrefix)
    val expected = None
    assert(fetcher.requestUrl == expected)
  }

  it should "return None if oclcId is missing" in {
    val fetcher = new HathiThumbnailFetcher(hathiIdOpt, None, None, googlePrefix)
    val expected = None
    assert(fetcher.requestUrl == expected)
  }

  it should "return None if googlePrefix is missing" in {
    val fetcher = new HathiThumbnailFetcher(hathiIdOpt, oclcIdOpt, None, None)
    val expected = None
    assert(fetcher.requestUrl == expected)
  }

  it should "extract the correct thumbnail URL from google response" in {
    val fetcher = new HathiThumbnailFetcher(hathiIdOpt, oclcIdOpt, isbnIdOpt, googlePrefix)
    val expected = Some("https://books.google.com/books/content?id=Yv-ZJZkIpgcC\u0026printsec=frontcover\u0026img=1\u0026zoom=5\u0026edge=curl")
    val parsed = fetcher.parseResponse(response)
    assert(fetcher.extractUrl(parsed.get) == expected)
  }
}
