package keys

const ( //Components keys
	//ViewData represents component view data response
	ViewData = "view"

	//ViewSummaryData represents view summary data output key
	ViewSummaryData = "summary"

	//SQL represents component expanded SQL
	SQL = "sql"

	//Status represents response status output key
	Status = "status"

	Error = "error"

	StatusCode = "status.code"
)

const ( //Response keys

	Response             = "response"
	ResponseElapsedInSec = "response.elapsedinsec"

	ResponseElapsedInMs = "response.elapsedinms"

	ResponseTime = "response.time"

	ResponseUnixTimeInSec = "response.unixtimeinsec"
)

const (
	Filter  = "filter"
	Filters = "filters"
)
