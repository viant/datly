package types

import (
	"fmt"
	"github.com/viant/structql"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type Mapper struct {
	from reflect.Type
	to   reflect.Type

	returnSingle bool
	query        *structql.Query
}

func NewMapper(from reflect.Type, to reflect.Type) (*Mapper, error) {
	mapper := &Mapper{
		from:         from,
		to:           to,
		returnSingle: to.Kind() != reflect.Slice && to.Kind() != reflect.Array,
	}

	return mapper, mapper.Init()
}

func (m *Mapper) Init() error {
	actualType := Elem(m.to)
	if actualType.Kind() != reflect.Struct || actualType == xreflect.TimeType {
		return fmt.Errorf("unsupported map type from %v to %v", m.from.String(), m.to.String())
	}

	selectStmt := &strings.Builder{}
	selectStmt.WriteString("SELECT ")
	fieldsNum := actualType.NumField()

	for i := 0; i < fieldsNum; i++ {
		if i != 0 {
			selectStmt.WriteString(", ")
		}

		selectStmt.WriteString(actualType.Field(i).Name)
	}

	selectStmt.WriteString(" FROM `/`")

	query, err := structql.NewQuery(selectStmt.String(), m.from, m.to)
	if err != nil {
		return err
	}

	m.query = query
	return nil
}

func (m *Mapper) Map(src interface{}) (interface{}, error) {
	if m.returnSingle {
		return m.query.First(src)
	}

	return m.query.Select(src)
}
