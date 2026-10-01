package dpla.ingestion3.harvesters.oai

import org.scalatest.flatspec.AnyFlatSpec

class IthakaOaiHarvesterTest extends AnyFlatSpec {

  // ── isApprovedHost ─────────────────────────────────────────────────────────

  "isApprovedHost" should "accept approved apex domains" in {
    assert(IthakaOaiHarvester.isApprovedHost("jstor.org"))
    assert(IthakaOaiHarvester.isApprovedHost("ithaka.org"))
    assert(IthakaOaiHarvester.isApprovedHost("artstor.org"))
  }

  it should "accept subdomains of approved apex domains" in {
    assert(IthakaOaiHarvester.isApprovedHost("media.ithaka.org"))
    assert(IthakaOaiHarvester.isApprovedHost("api.jstor.org"))
    assert(IthakaOaiHarvester.isApprovedHost("cdn.artstor.org"))
  }

  it should "reject unapproved hosts" in {
    assert(!IthakaOaiHarvester.isApprovedHost("example.com"))
    assert(!IthakaOaiHarvester.isApprovedHost("evil.jstor.org.attacker.com"))
    assert(!IthakaOaiHarvester.isApprovedHost("notjstor.org"))
  }

  it should "reject null" in {
    assert(!IthakaOaiHarvester.isApprovedHost(null))
  }

  // ── isPrivateHost ──────────────────────────────────────────────────────────

  "isPrivateHost" should "detect loopback addresses" in {
    assert(IthakaOaiHarvester.isPrivateHost("127.0.0.1"))
    assert(IthakaOaiHarvester.isPrivateHost("127.1.2.3"))
    assert(IthakaOaiHarvester.isPrivateHost("localhost"))
    assert(IthakaOaiHarvester.isPrivateHost("foo.localhost"))
    assert(IthakaOaiHarvester.isPrivateHost("::1"))
    assert(IthakaOaiHarvester.isPrivateHost("[::1]"))
  }

  it should "detect RFC-1918 private ranges" in {
    assert(IthakaOaiHarvester.isPrivateHost("10.0.0.1"))
    assert(IthakaOaiHarvester.isPrivateHost("10.255.255.255"))
    assert(IthakaOaiHarvester.isPrivateHost("192.168.1.1"))
    assert(IthakaOaiHarvester.isPrivateHost("172.16.0.1"))
    assert(IthakaOaiHarvester.isPrivateHost("172.31.255.255"))
  }

  it should "detect link-local addresses" in {
    assert(IthakaOaiHarvester.isPrivateHost("169.254.0.1"))
    assert(IthakaOaiHarvester.isPrivateHost("foo.local"))
    assert(IthakaOaiHarvester.isPrivateHost("host.internal"))
  }

  it should "allow public IP addresses" in {
    assert(!IthakaOaiHarvester.isPrivateHost("8.8.8.8"))
    assert(!IthakaOaiHarvester.isPrivateHost("151.101.1.1"))
    assert(!IthakaOaiHarvester.isPrivateHost("media.ithaka.org"))
  }

  it should "treat null as private" in {
    assert(IthakaOaiHarvester.isPrivateHost(null))
  }

  // ── isValidInitialUrl ──────────────────────────────────────────────────────

  "isValidInitialUrl" should "accept HTTPS URLs on approved hosts" in {
    assert(IthakaOaiHarvester.isValidInitialUrl("https://media.ithaka.org/medias/123"))
    assert(IthakaOaiHarvester.isValidInitialUrl("https://api.jstor.org/medias/123"))
  }

  it should "reject HTTP initial URLs" in {
    assert(!IthakaOaiHarvester.isValidInitialUrl("http://media.ithaka.org/medias/123"))
  }

  it should "reject unapproved hosts" in {
    assert(!IthakaOaiHarvester.isValidInitialUrl("https://example.com/medias/123"))
    assert(!IthakaOaiHarvester.isValidInitialUrl("https://attacker.com/medias/123"))
  }

  it should "reject private initial URLs" in {
    assert(!IthakaOaiHarvester.isValidInitialUrl("https://192.168.1.1/medias/123"))
    assert(!IthakaOaiHarvester.isValidInitialUrl("https://10.0.0.1/medias/123"))
    assert(!IthakaOaiHarvester.isValidInitialUrl("https://localhost/medias/123"))
  }

  it should "reject malformed URLs" in {
    assert(!IthakaOaiHarvester.isValidInitialUrl("not a url"))
    assert(!IthakaOaiHarvester.isValidInitialUrl(""))
  }

  // ── isValidRedirectUrl ─────────────────────────────────────────────────────

  "isValidRedirectUrl" should "accept HTTPS redirect targets on approved hosts" in {
    assert(IthakaOaiHarvester.isValidRedirectUrl("https://media.ithaka.org/file.jpg"))
  }

  it should "accept HTTP redirect targets on approved hosts (JSTOR downgrades https→http)" in {
    assert(IthakaOaiHarvester.isValidRedirectUrl("http://media.ithaka.org/file.jpg"))
  }

  it should "reject redirect targets on unapproved hosts" in {
    assert(!IthakaOaiHarvester.isValidRedirectUrl("https://example.com/file.jpg"))
    assert(!IthakaOaiHarvester.isValidRedirectUrl("http://attacker.com/file.jpg"))
  }

  it should "reject redirect targets pointing to private addresses" in {
    assert(!IthakaOaiHarvester.isValidRedirectUrl("http://192.168.1.1/internal"))
    assert(!IthakaOaiHarvester.isValidRedirectUrl("http://10.0.0.1/secret"))
    assert(!IthakaOaiHarvester.isValidRedirectUrl("http://localhost/admin"))
  }

  it should "reject non-HTTP(S) schemes in redirect targets" in {
    assert(!IthakaOaiHarvester.isValidRedirectUrl("file:///etc/passwd"))
    assert(!IthakaOaiHarvester.isValidRedirectUrl("ftp://media.ithaka.org/file"))
  }

  // ── decodeXmlEntities ──────────────────────────────────────────────────────

  "decodeXmlEntities" should "decode standard XML entities" in {
    assert(IthakaOaiHarvester.decodeXmlEntities("a&amp;b") == "a&b")
    assert(IthakaOaiHarvester.decodeXmlEntities("&lt;tag&gt;") == "<tag>")
    assert(IthakaOaiHarvester.decodeXmlEntities("say &quot;hi&quot;") == "say \"hi\"")
    assert(IthakaOaiHarvester.decodeXmlEntities("it&apos;s") == "it's")
  }

  it should "leave strings without entities unchanged" in {
    val url = "https://media.ithaka.org/medias/123?foo=bar"
    assert(IthakaOaiHarvester.decodeXmlEntities(url) == url)
  }

  it should "decode a URL with &amp; in the query string" in {
    val raw = "https://media.ithaka.org/medias?a=1&amp;b=2"
    assert(IthakaOaiHarvester.decodeXmlEntities(raw) == "https://media.ithaka.org/medias?a=1&b=2")
  }
}
