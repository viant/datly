package data

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/shared"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//View represents a data View
type View struct {
	shared.Reference
	Connector *config.Connector
	Name      string
	Alias     string `json:",omitempty"`
	Table     string `json:",omitempty"`
	From      string `json:",omitempty"`

	Exclude    []string  `json:",omitempty"`
	Columns    []*Column `json:",omitempty"`
	CaseFormat string    `json:",omitempty"`

	Criteria  *data.Criteria `json:",omitempty"`
	Selector  Config         `json:",omitempty"`
	Prefix    string
	Component *Component

	With      []*Relation `json:",omitempty"`
	_columns  map[string]*Column
	_excluded map[string]bool

	placeholdersReferences map[string]*Relation
	initialized            bool
	isValid                bool
	typeRebuilt            bool
	Caser                  format.Case
	newCollector           func(session *Session) *Collector
	destIndex              int
}

//DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Component.Type()
}

func (v *View) Init(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	if v.initialized {
		return nil
	}

	err := v.init(ctx, views, connectors, types)
	if err == nil {
		v.initialized = true
	}

	return err
}

func (v *View) init(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	if v.Name == v.Ref {
		return fmt.Errorf("view name and ref cannot be the same")
	}

	if v.Name == "" {
		return fmt.Errorf("view name was empty")
	}

	if v.Ref != "" {
		view, err := views.Lookup(v.Ref)
		if err != nil {
			return err
		}

		err = view.Init(ctx, views, connectors, types)
		if err != nil {
			return err
		}

		v.inherit(view)
	}

	v.Alias = notEmptyOf(v.Alias, "t")
	if v.Connector == nil {
		return fmt.Errorf("connector was empty")
	}

	err := v.Connector.Init(ctx, connectors)
	if err != nil {
		return err
	}

	err = v.Connector.Validate()
	if err != nil {
		return err
	}

	if v.Table == "" {
		v.Table = v.Name
	}

	v.ensureIndexExcluded()
	err = v.ensureColumns(ctx)
	v._columns = Columns(v.Columns).Index()
	if err != nil {
		return err
	}

	err = v.initRelations(ctx, views, connectors, types)
	if err != nil {
		return err
	}

	if err := v.ensureCaseFormat(); err != nil {
		return err
	}
	v.ensureComponent(types)
	v.initializeDestIndex(0)
	if err != nil {
		return err
	}

	v.ensureCollector()
	return v.initHolders()
}

func (v *View) initRelations(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	var err error
	if len(v.With) == 0 {
		return nil
	}

	for i := range v.With {

		err = v.With[i].Init(ctx, views, connectors, types)
		if err != nil {
			return err
		}

	}
	return nil
}

//TODO: Split it to 2 methods, ensure _columns and types
func (v *View) ensureColumns(ctx context.Context) error {
	if len(v.Columns) != 0 {
		return Columns(v.Columns).Init()
	}

	db, err := v.Connector.Db()
	if err != nil {
		return err
	}

	query, err := db.QueryContext(ctx, "SELECT * FROM "+v.Source()+" WHERE 1=2")
	if err != nil {
		return err
	}

	types, err := query.ColumnTypes()
	if err != nil {
		return err
	}

	ioColumns := v.exclude(io.TypesToColumns(types))
	v.Columns = convertIoColumnsToColumns(ioColumns)
	return nil
}

func convertIoColumnsToColumns(ioColumns []io.Column) []*Column {
	columns := make([]*Column, 0)
	for i := 0; i < len(ioColumns); i++ {
		columns = append(columns, &Column{
			Name:     ioColumns[i].Name(),
			DataType: ioColumns[i].ScanType().Name(),
			rType:    ioColumns[i].ScanType(),
		})
	}
	return columns
}

func (v *View) ColumnByName(name string) (*Column, bool) {
	v.createColumnMapIfNeeded(false)

	if column, ok := v._columns[name]; ok {
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

func (v *View) ensureComponent(types Types) {
	if v.Component == nil {
		v.Component = &Component{
			Name: v.Name,
		}
	}

	if v.Component.Name != "" {
		componentType := types.Lookup(v.Component.Name)
		if componentType != nil {
			v.Component.setType(componentType)
		}
	}

	v.Component.Init(v.Columns, v.With, v.Caser)
}

func (v *View) createColumnMapIfNeeded(force bool) {
	if v._columns != nil && !force {
		return
	}

	v._columns = make(map[string]*Column)
	for i := range v.Columns {
		v._columns[v.Columns[i].Name] = v.Columns[i]
		v._columns[strings.Title(v.Columns[i].Name)] = v.Columns[i]
		v._columns[strings.ToLower(v.Columns[i].Name)] = v.Columns[i]
		v._columns[strings.ToUpper(v.Columns[i].Name)] = v.Columns[i]
	}
}

func (v *View) Db() (*sql.DB, error) {
	return v.Connector.Db()
}

func (v *View) exclude(columns []io.Column) []io.Column {
	if len(v.Exclude) == 0 {
		return columns
	}

	filtered := make([]io.Column, 0, len(columns))
	for i := range columns {
		//TODO: add method that normalizes the keys
		if _, ok := v._excluded[columns[i].Name()]; ok {
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

	v.placeholdersReferences = make(map[string]*Relation)
	for i := range v.With {
		v.placeholdersReferences[strings.Title(v.With[i].Name)] = v.With[i]
	}
}

func (v *View) inherit(view *View) {
	if v.Connector == nil {
		v.Connector = view.Connector
	}

	v.Alias = notEmptyOf(v.Alias, view.Alias)
	v.Table = notEmptyOf(v.Table, view.Table)
	v.From = notEmptyOf(v.From, view.From)

	if len(v.Columns) == 0 {
		v.Columns = view.Columns
	}

	if v.Criteria == nil {
		v.Criteria = view.Criteria
	}

	if len(v.With) == 0 {
		v.With = view.With
	}

	if v.Component == nil && len(v.With) == 0 {
		v.Component = view.Component
	}

	if len(v.Exclude) == 0 {
		v.Exclude = view.Exclude
	}

	if v.CaseFormat == "" {
		v.CaseFormat = view.CaseFormat
		v.Caser = view.Caser
	}

	if v.newCollector == nil && len(v.With) == 0 {
		v.newCollector = view.newCollector
	}
}

func (v *View) ensureIndexExcluded() {
	if len(v.Exclude) == 0 {
		return
	}

	v._excluded = Names(v.Exclude).Index()
}

func (v *View) SelectedColumns(selector *Selector) ([]*Column, error) {
	if selector == nil || len(selector.Columns) == 0 {
		return v.Columns, nil
	}

	result := make([]*Column, len(selector.Columns))
	for i, name := range selector.Columns {
		column, ok := v._columns[name]
		if !ok {
			return nil, fmt.Errorf("invalid column name: %v", name)
		}
		result[i] = column
	}
	return result, nil
}

func (v *View) ensureCaseFormat() error {
	if v.CaseFormat == "" {
		v.CaseFormat = "lu"
	}
	var err error
	v.Caser, err = format.NewCase(v.CaseFormat)
	return err
}

func (v *View) ensureCollector() {
	relations := Relations(v.With).PopulateWithResolve()
	resolverColumns := Relations(relations).Columns()

	v.newCollector = func(session *Session) *Collector {
		if session.AllowUnmapped {
			return NewCollector(nil, v.Component.slice, v, session)
		}
		return NewCollector(resolverColumns, v.Component.slice, v, session)
	}
}

func (v *View) Collector(session *Session) *Collector {
	return v.newCollector(session)
}

func notEmptyOf(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func (v *View) DestCount() int {
	counter := 1
	if len(v.With) == 0 {
		return counter
	}

	for i := range v.With {
		counter += v.With[i].Of.DestCount()
	}

	return counter
}

func (v *View) initializeDestIndex(count int) {
	v.destIndex = count
	count++

	if len(v.With) == 0 {
		return
	}

	for i := range v.With {
		v.With[i].Of.initializeDestIndex(count)
	}
}

func (v *View) DestIndex() int {
	return v.destIndex
}

func (v *View) UseTransientSlice() bool {
	return v.destIndex == 0 || len(v.With) > 0
}

func (v *View) initHolders() error {
	if len(v.With) == 0 {
		return nil
	}

	for i := range v.With {
		relation := v.With[i]

		relation.holderField = xunsafe.FieldByName(v.DataType(), relation.Holder)
		if relation.holderField == nil {
			return fmt.Errorf("failed to lookup holderField %v", relation.Holder)
		}

		columnName := v.Caser.Format(relation.Column, format.CaseUpperCamel)
		relation.columnField = xunsafe.FieldByName(v.DataType(), columnName)

		relation.HasColumnField = relation.columnField != nil
		if relation.Cardinality == "Many" && !relation.HasColumnField {
			return fmt.Errorf("column %v doesn't have corresponding field in the struct: %v", columnName, v.DataType().String())
		}
	}

	return nil
}
