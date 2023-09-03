package content

import "github.com/viant/sqlx/io/load/reader/csv"

type CSVConfig struct {
	Separator        string
	NullValue        string
	_config          *csv.Config
	InputMarshaller  *csv.Marshaller
	OutputMarshaller *csv.Marshaller
}
