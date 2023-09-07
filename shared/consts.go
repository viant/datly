package shared

// BuiltInKey represents keys that are provided as parameters for every view.View in view.Session
type BuiltInKey string

const (
	//DataViewName represents View.DbName parameter
	DataViewName BuiltInKey = "session.View.DbName"
	//SubjectName represents PrincialSubject parameter
	SubjectName BuiltInKey = "session.PrincialSubject"
)
