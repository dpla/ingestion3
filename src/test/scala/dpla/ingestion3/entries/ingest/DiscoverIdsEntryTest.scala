package dpla.ingestion3.entries.ingest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class DiscoverIdsEntryTest extends AnyFlatSpec with Matchers {

  "isEnabled" should "accept the usual affirmatives" in {
    Seq("true", "TRUE", " yes ", "on", "1").foreach { v =>
      withClue(s"'$v': ")(DiscoverIdsEntry.isEnabled(Some(v)) shouldBe true)
    }
  }

  it should "treat absent, false and anything unrecognised as disabled" in {
    // A typo must switch a hub off rather than on: an unexpectedly disabled hub
    // is visible in the run summary, an unexpectedly enabled one hits a
    // partner's endpoint unannounced.
    Seq(None, Some("false"), Some(""), Some("ture"), Some("maybe")).foreach { v =>
      withClue(s"$v: ")(DiscoverIdsEntry.isEnabled(v) shouldBe false)
    }
  }

  "enabledHubs" should "list only hubs switched on" in {
    val conf = writeTempConf(
      """getty.provider = "Getty"
        |getty.harvest.discovery.enabled = false
        |mississippi.provider = "MDL"
        |mississippi.harvest.discovery.enabled = true
        |mwdl.provider = "MWDL"
        |ohio.provider = "Ohio"
        |ohio.harvest.type = "oai"
        |""".stripMargin
    )
    try DiscoverIdsEntry.enabledHubs(conf.toString) shouldBe Seq("mississippi")
    finally Files.deleteIfExists(conf)
  }

  it should "return empty when no hub is enabled" in {
    val conf = writeTempConf("""ohio.harvest.type = "oai"""")
    try DiscoverIdsEntry.enabledHubs(conf.toString) shouldBe Seq.empty
    finally Files.deleteIfExists(conf)
  }

  it should "list every enabled hub, sorted" in {
    val conf = writeTempConf(
      """zeta.harvest.discovery.enabled = true
        |alpha.harvest.discovery.enabled = true
        |""".stripMargin
    )
    try DiscoverIdsEntry.enabledHubs(conf.toString) shouldBe Seq("alpha", "zeta")
    finally Files.deleteIfExists(conf)
  }

  it should "accept an unquoted HOCON boolean" in {
    val conf = writeTempConf("""a.harvest.discovery.enabled = true""")
    try DiscoverIdsEntry.enabledHubs(conf.toString) shouldBe Seq("a")
    finally Files.deleteIfExists(conf)
  }

  it should "not be tripped by a hub key that is a plain string" in {
    // Top-level scalars exist in i3.conf; asking them for a nested path throws,
    // and that must not take the whole enumeration down.
    val conf = writeTempConf(
      """someGlobal = "value"
        |mississippi.harvest.discovery.enabled = true
        |""".stripMargin
    )
    try DiscoverIdsEntry.enabledHubs(conf.toString) shouldBe Seq("mississippi")
    finally Files.deleteIfExists(conf)
  }

  it should "raise rather than guess when the conf file is missing" in {
    a[RuntimeException] should be thrownBy
      DiscoverIdsEntry.enabledHubs("/nonexistent/path/to/i3.conf")
  }

  private def writeTempConf(contents: String): Path = {
    val p = Files.createTempFile("discovery-test", ".conf")
    Files.write(p, contents.getBytes(StandardCharsets.UTF_8))
    p
  }
}
