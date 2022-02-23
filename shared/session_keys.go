package shared

//BuiltInKey represents keys that are provided as parameters for every data.View in data.Session
type BuiltInKey string

const (
	//DataViewName represents View.Name parameter
	DataViewName BuiltInKey = "session.View.Name"
	//SubjectName represents Subject parameter
	SubjectName BuiltInKey = "session.Subject"
)
