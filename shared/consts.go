package shared

//BuiltInKey represents keys that are provided as parameters for every view.View in view.Session
type BuiltInKey string

const (
	//DataViewName represents View.Name parameter
	DataViewName BuiltInKey = "session.View.Name"
	//SubjectName represents Subject parameter
	SubjectName BuiltInKey = "session.Subject"
)

type SqlPosition string

const (
	ColumnInPosition      SqlPosition = "$COLUMN_IN"
	WhereColumnInPosition SqlPosition = "$WHERE_COLUMN_IN"
	Criteria              SqlPosition = "$CRITERIA"
	Pagination            SqlPosition = "$PAGINATION"
)
