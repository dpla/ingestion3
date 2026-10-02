package dpla.ingestion3.harvesters

/** Mixin for harvesters that need to read the hub's previous harvest.
  *
  * Most harvesters are told everything they need by `i3Conf`. A harvester that
  * seeds from its own prior output needs one more thing: the data root that
  * [[dpla.ingestion3.dataStorage.OutputHelper]] writes activity directories
  * under. That value is a run parameter (`--output`), not configuration -- it is
  * the same for every hub and never edited -- so it does not belong in i3.conf,
  * and pinning a specific previous harvest there would mean rewriting config
  * after every ingest.
  *
  * [[dpla.ingestion3.executors.HarvestExecutor]] supplies it to any harvester
  * that mixes this in, and ignores everything else.
  */
trait SeedsFromPreviousHarvest {

  private var root: Option[String] = None

  /** Output root for this run (`--output`), once the executor has supplied it. */
  def dataRoot: Option[String] = root

  def dataRoot_=(value: String): Unit =
    root = Option(value).map(_.trim).filter(_.nonEmpty)
}
