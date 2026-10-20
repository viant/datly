package contracts

import "github.com/viant/datly/transcribe/testdata/borrowedinput/rows"

type LocalRow struct {
	ID int `sqlx:"id"`
}
type Input struct {
	Local []*LocalRow        `parameter:"Local,kind=view,in=Current,dataType=[]*LocalRow,cardinality=many" view:"Current,type=LocalRow,connector=main,table=borrowed_rows,selectorNoLimit=true" sql:"uri=borrowed_contract:sql/current.sql"`
	Third []*rows.ForeignRow `parameter:"Third,kind=view,in=Other,dataType=[]*ForeignRow,cardinality=many" view:"Other,type=ForeignRow,connector=main,table=borrowed_rows,selectorNoLimit=true" sql:"uri=borrowed_contract:sql/other.sql"`
}
type Output struct {
	Status string `parameter:"Status,kind=output,in=status"`
}
