package data

import (
	"database/sql"
	"github.com/pkg/errors"
	"github.com/viant/datly/data"
	"github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

//View represents a data View
type View struct {
	Connector string
	RefId     string
	Name      string
	Alias     string    `json:",omitempty"`
	Table     string    `json:",omitempty"`
	From      string    `json:",omitempty"`
	Columns   []*Column `json:",omitempty"`

	Criteria *data.Criteria `json:",omitempty"`
	Selector Selector       `json:",omitempty"`

	PrimaryKey []string `json:",omitempty"`
	Mutable    *bool    `json:",omitempty"`
	Component  *Component
	columns    map[string]*Column
}

//DataType returns struct type.
//struct type always will be a pointer
func (v *View) DataType() reflect.Type {
	return v.Component.ComponentType()
}

func (v *View) EnsureColumns(db *sql.DB) error {
	v.ensureComponent()
	if len(v.Columns) == 0 || v.Component.compType == nil {
		err := v.ensureColumnsAndType(db)
		if err != nil {
			return err
		}
	}

	v.ensureSelectorColumns()
	return nil
}

func (v *View) ensureColumnsAndType(db *sql.DB) error {
	query, err := db.Query("SELECT * FROM " + v.Source() + " WHERE 1=2")
	if err != nil {
		return err
	}

	types, err := query.ColumnTypes()
	if err != nil {
		return err
	}

	ioColumns := io.TypesToColumns(types)
	v.Component.ensureType(ioColumns, &v.Selector)

	if len(v.Columns) != 0 {
		return nil
	}

	columnsLen := len(types)
	columns := make([]*Column, columnsLen)
	for i := 0; i < columnsLen; i++ {
		columns[i] = &Column{
			Name:     strings.Title(ioColumns[i].Name()),
			DataType: ioColumns[i].ScanType().Name(),
		}
	}
	v.Columns = columns
	return nil
}

func (v *View) ColumnByName(name string) (*Column, bool) {
	v.createColumnMapIfNeeded()

	if column, ok := v.columns[name]; ok {
		return column, true
	}

	return nil, false
}

func (v *View) Source() string {
	if v.From != "" {
		return v.From
	}

	if v.Table != "" {
		return v.Table
	}

	return v.Name
}

func (v *View) ensureComponent() {
	if v.Component != nil {
		return
	}

	v.Component = &Component{
		Name: v.Name,
	}
}

func (v *View) ensureSelectorColumns() {
	if v.Selector.Columns != nil && len(v.Selector.Columns) != 0 {
		return
	}

	v.Selector.SetColumns(v.Columns)
}

func (v *View) MergeWithSelector(selector *Selector) (*View, error) {
	var found bool
	for i := range selector.Columns {
		if _, found = v.ColumnByName(selector.Columns[i]); !found {
			return nil, errors.New("couldn't merge selector because column: " + selector.Columns[i] + " is not present in View columns")
		}
	}

	if _, found = v.ColumnByName(selector.OrderBy); !found {
		return nil, errors.New("couldn't merge selector because column: " + selector.OrderBy + " is not present in View columns")
	}

	newView := *v
	//TODO: move criteria and excluded out of the Selector
	newView.Selector = *selector

	return &newView, nil
}

func (v *View) createColumnMapIfNeeded() {
	if v.columns != nil {
		return
	}

	v.columns = make(map[string]*Column)
	for i := range v.Columns {
		v.columns[v.Columns[i].Name] = v.Columns[i]
	}
}
