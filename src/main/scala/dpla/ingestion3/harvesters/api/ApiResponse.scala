package dpla.ingestion3.harvesters.api

// Represents a single page response from an API request
sealed trait ApiResponse

case class ApiError(message: String, errorSource: ApiSource) extends ApiResponse

case class ApiRecord(id: String, document: String) extends ApiResponse

case class ApiSource(
                      queryParams: Map[String, String],
                      url: Option[String] = None,
                      text: Option[String] = None
                    ) extends ApiResponse // todo wtf
