package data

type Request struct {
	Columns    []string
	Prefix     string
	OrderBy    string
	Offset     int
	CaseFormat string
	Limit      int
	OmitEmpty  bool
}
