package dpla.ingestion3.harvesters

import org.scalatest.flatspec.AnyFlatSpec

/** Tests for [[LocalHarvester.s3SubCmdFor]], which picks how file harvesters
  * (FlFileHarvester, OaiFileHarvester) fetch an s3:// delivery. A file hub's
  * delivery is chosen at harvest time and can be a single dated file or a
  * dated folder, so the harvester has to handle both.
  */
class LocalHarvesterTest extends AnyFlatSpec {

  import LocalHarvester.s3SubCmdFor

  "s3SubCmdFor" should "copy a single-object delivery" in {
    assert(s3SubCmdFor("s3://dpla-hub-fl/SSDN-2026-05-24.jsonl") == "cp")
    assert(s3SubCmdFor("s3://dpla-hub-nypl/nypl-20260820.zip") == "cp")
    assert(s3SubCmdFor("s3://dpla-hub-ohio/ohio-2026-11-02.xml.gz") == "cp")
    assert(s3SubCmdFor("s3://dpla-hub-vt/20240529_gmda_allRecords.ZIP") == "cp")
  }

  it should "sync a folder delivery" in {
    assert(s3SubCmdFor("s3://dpla-hub-ohio/2026-09-16/") == "sync")
    assert(s3SubCmdFor("s3://dpla-hub-ga/09212026/") == "sync")
    assert(s3SubCmdFor("s3://dpla-hub-tdl/2026-10-23") == "sync")
  }

  it should "sync a folder whose name ends like a file once it has a trailing slash" in {
    assert(s3SubCmdFor("s3://dpla-hub-ohio/export.zip/") == "sync")
  }

  it should "leave local paths alone (sync is ignored for them)" in {
    assert(s3SubCmdFor("/mnt/data/ct/originalRecords/file.zip") == "sync")
  }
}
