package data

import (
	"database/sql"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/v1/config"
	"github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

//View represents a data View
type View struct {
	Connector string
	connector *config.Connector
	RefNames  []string
	Name      string
	Alias     string    `json:",omitempty"`
	Table     string    `json:",omitempty"`
	From      string    `json:",omitempty"`
	Columns   []*Column `json:",omitempty"`

	Criteria *data.Criteria `json:",omitempty"`
	Default  *Config        `json:",omitempty"`

	PrimaryKey []string     `json:",omitempty"`
	Mutable    *bool        `json:",omitempty"`
	References []*Reference `json:",omitempty"`
	Component  *Component

	ExcludedColumns []string `json:",omitempty"`

	columns         map[string]*Column
	excludedColumns map[string]bool

	placeholdersReferences map[string]*Reference
	initialized            bool
	isValid                bool
	typeRebuilt            bool
}

//DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Component.ComponentType()
}

func (v *View) init() error {
	v.ensureConfig()
	v.ensurePlaceholderReferences()
	db, err := v.connector.Db()
	if err != nil {
		return err
	}

	err = v.ensureColumns(db)
	if err != nil {
		return err
	}

	v.initialized = true
	v.isValid = true
	v.createColumnMapIfNeeded(true)
	return nil
}

func (v *View) ensureColumns(db *sql.DB) error {
	v.ensureComponent()
	if len(v.Columns) == 0 || v.Component.compType == nil {
		err := v.ensureColumnsAndType(db)
		if err != nil {
			return err
		}
	}

	return v.Default.ensureColumns(v.Columns)
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

	ioColumns := v.exclude(io.TypesToColumns(types))
	v.Component.ensureType(ioColumns)

	if len(v.Columns) != 0 {
		return nil
	}

	columns := make([]*Column, 0)
	excludedColumns := v.excludedColumnsAsMap()
	for i := 0; i < len(ioColumns); i++ {
		columnName := strings.Title(ioColumns[i].Name())
		if _, ok := excludedColumns[columnName]; ok {
			continue
		}
		columns = append(columns, &Column{
			Name:     columnName,
			DataType: ioColumns[i].ScanType().Name(),
		})
	}
	v.Columns = columns
	return nil
}

func (v *View) ColumnByName(name string) (*Column, bool) {
	v.createColumnMapIfNeeded(false)

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

func (v *View) excludedColumnsAsMap() map[string]bool {
	if v.excludedColumns != nil {
		return v.excludedColumns
	}

	excluded := make(map[string]bool)
	excludedLen := len(v.ExcludedColumns)
	for i := 0; i < excludedLen; i++ {
		excluded[strings.Title(v.ExcludedColumns[i])] = true
		excluded[v.ExcludedColumns[i]] = true
		excluded[strings.ToLower(v.ExcludedColumns[i])] = true
		excluded[strings.ToUpper(v.ExcludedColumns[i])] = true
	}
	v.excludedColumns = excluded
	return excluded
}

func (v *View) createColumnMapIfNeeded(force bool) {
	if v.columns != nil && !force {
		return
	}

	v.columns = make(map[string]*Column)
	for i := range v.Columns {
		v.columns[v.Columns[i].Name] = v.Columns[i]
		v.columns[strings.Title(v.Columns[i].Name)] = v.Columns[i]
		v.columns[strings.ToLower(v.Columns[i].Name)] = v.Columns[i]
		v.columns[strings.ToUpper(v.Columns[i].Name)] = v.Columns[i]
	}
}

func (v *View) Db() (*sql.DB, error) {
	return v.connector.Db()
}

func (v *View) IsValid() error {
	if v.isValid && v.typeRebuilt {
		return nil
	}

	var err error
	if !v.typeRebuilt {
		err = v.rebuildType()
		if err != nil {
			return err
		}
	}

	if v.connector == nil {
		return fmt.Errorf("view %v has no connector", v.Name)
	}

	if len(v.Columns) == 0 {
		return fmt.Errorf("view %v has empty columns", v.Name)
	}

	if len(v.Default.Columns) == 0 {
		return fmt.Errorf("view %v has empty selector columns", v.Name)
	}

	for i := range v.References {
		if v.References[i].Child == nil {
			return fmt.Errorf("view %v has specified reference but has no child", v.Name)
		}
	}

	for i := range v.References {
		if err != nil {
			return err
		}

		err = v.References[i].Child.IsValid()
	}
	return err
}

func (v *View) exclude(columns []io.Column) []io.Column {
	excluded := v.excludedColumnsAsMap()

	filtered := make([]io.Column, 0)
	for i := range columns {
		if _, ok := excluded[strings.Title(columns[i].Name())]; ok {
			continue
		}
		filtered = append(filtered, columns[i])
	}
	return filtered
}

func (v *View) ensurePlaceholderReferences() {
	if v.placeholdersReferences != nil {
		return
	}

	v.placeholdersReferences = make(map[string]*Reference)
	for i := range v.References {
		v.placeholdersReferences[strings.Title(v.References[i].Name)] = v.References[i]
	}
}

func (v *View) Validate() error {
	if v.initialized {
		return nil
	}

	if v.isValid {
		return nil
	}

	var err error
	forEachReferenceView(v, func(view *View) bool {
		if v.connector == nil {
			err = fmt.Errorf("not found connector %v for %v", v.Connector, v.Name)
			return false
		}

		if len(v.RefNames) != 0 && len(v.RefNames) != len(v.References) {
			err = fmt.Errorf("some of the view %v references are missing", v.Name)
			return false
		}
		return true

	}, func(reference *Reference) bool {
		if reference.Child == nil {
			err = fmt.Errorf("reference %v has no child view", reference.Name)
			return false
		}
		return true
	})

	if err == nil {
		v.initialized = true
	}
	return err
}

func (v *View) RequestSelector(request *Request) (*ClientSelector, error) {
	v.createColumnMapIfNeeded(false)

	var columns []*Column
	var rType reflect.Type
	var err error
	if len(request.Columns) == 0 {
		columns = v.Columns
		rType = v.Default.rType
	} else {
		columns = make([]*Column, len(request.Columns))
		structFields := make([]reflect.StructField, len(request.Columns))
		var columnType reflect.Type
		for i := range request.Columns {
			if column, ok := v.columns[request.Columns[i]]; ok {
				columns[i] = column
				if columnType, err = columns[i].Type(); err == nil {
					structFields[i] = reflect.StructField{
						Name: strings.Title(column.Name),
						Type: columnType,
					}
				} else {
					return nil, err
				}

				continue
			}
			return nil, fmt.Errorf("not found column %v in view", request.Columns[i])
		}
		rType = reflect.StructOf(structFields)
	}

	offset := v.Default.Offset
	if request.Offset > 0 {
		offset = request.Offset
	}

	orderBy := v.Default.OrderBy
	if request.OrderBy != "" {
		if column, ok := v.columns[request.OrderBy]; ok {
			orderBy = column.Name
		} else {
			return nil, fmt.Errorf("not found orderBy %v in view %v", request.OrderBy, v.Name)
		}
	}

	limit := v.Default.Limit
	if request.Limit != 0 {
		limit = request.Limit
	}

	return &ClientSelector{
		columns: columns,
		OrderBy: orderBy,
		Offset:  offset,
		Limit:   limit,
		rType:   rType,
	}, nil
}

func (v *View) ensureConfig() {
	if v.Default != nil {
		return
	}

	v.Default = &Config{}
}

func (v *View) rebuildType() error {
	if v.typeRebuilt || len(v.References) == 0 {
		return nil
	}

	for i := range v.References {
		if err := v.References[i].Child.rebuildType(); err != nil {
			return err
		}
	}

	for i := range v.References {
		if v.References[i].Cardinality == "One" {
			if col, ok := v.columns[strings.Title(v.References[i].Column)]; ok {
				col.setReference(v.References[i])
			} else {
				return fmt.Errorf("not found column with name %v for reference %v ", v.References[i].Column, v.References[i].Name)
			}
		} else if v.References[i].Cardinality == "Many" {
			v.Columns = append(v.Columns, &Column{
				Name:      v.References[i].RefHolder,
				reference: v.References[i],
				rType:     reflect.SliceOf(v.References[i].Child.DataType()),
				structField: &reflect.StructField{
					Name: strings.Title(v.References[i].RefHolder),
					Type: reflect.SliceOf(v.References[i].Child.DataType()),
				},
			})
		}
	}

	newStructFields := make([]reflect.StructField, len(v.Columns))
	var strField *reflect.StructField
	var err error
	for i := range v.Columns {
		strField, err = v.Columns[i].StructField()
		if err != nil {
			return err
		}
		newStructFields[i] = *strField
	}

	v.Component.setType(reflect.StructOf(newStructFields))
	v.createColumnMapIfNeeded(true)
	v.typeRebuilt = true
	return nil
}
