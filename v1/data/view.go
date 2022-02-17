package data

import (
	"context"
	"database/sql"
	"fmt"
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

	Criteria   *Criteria    `json:",omitempty"`
	Selector   *Config      `json:",omitempty"`
	Parameters []*Parameter `json:",omitempty"`

	Prefix string  `json:",omitempty"`
	Schema *Schema `json:",omitempty"`

	With       []*Relation `json:",omitempty"`
	ParamField *xunsafe.Field

	MatchStrategy MatchStrategy `json:",omitempty"`
	BatchReadSize *int          `json:",omitempty"`

	_columns  map[string]*Column
	_excluded map[string]bool

	Caser        format.Case `json:",omitempty"`
	initialized  bool
	isValid      bool
	typeRebuilt  bool
	newCollector func(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector
}

//DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Schema.Type()
}

func (v *View) Init(ctx context.Context, resource *Resource) error {
	if v.initialized {
		return nil
	}

	err := v.init(ctx, resource)
	if err == nil {
		v.initialized = true
	}

	return err
}

func (v *View) init(ctx context.Context, resource *Resource) error {
	if v.Name == v.Ref {
		return fmt.Errorf("view name and ref cannot be the same")
	}

	if v.Name == "" {
		return fmt.Errorf("view name was empty")
	}

	if v.Ref != "" {
		view, err := resource._views.Lookup(v.Ref)
		if err != nil {
			return err
		}

		err = view.Init(ctx, resource)
		if err != nil {
			return err
		}

		v.inherit(view)
	}
	if v.Selector == nil {
		v.Selector = &Config{}
	}
	//v.destIndex = resource.AddAndIncrement()
	v.Alias = notEmptyOf(v.Alias, "t")
	if v.Connector == nil {
		return fmt.Errorf("connector was empty")
	}

	var err error
	if err = v.Connector.Init(ctx, resource._connectors); err != nil {
		return err
	}

	if err = v.Connector.Validate(); err != nil {
		return err
	}

	if v.Table == "" {
		v.Table = v.Name
	}

	v.ensureIndexExcluded()

	if err = v.ensureColumns(ctx); err != nil {
		return err
	}
	v._columns = Columns(v.Columns).Index()
	if err = v.initRelations(ctx, resource); err != nil {
		return err
	}

	if err := v.ensureCaseFormat(); err != nil {
		return err
	}
	v.ensureSchema(resource.types)
	if err = v.initParams(ctx, resource); err != nil {
		return err
	}

	v.ensureCollector()
	if err = v.initHolders(); err != nil {
		return err
	}

	if v.MatchStrategy == "" {
		v.MatchStrategy = ReadMatched
	}

	if err = v.MatchStrategy.Validate(); err != nil {
		return err
	}

	v.propagateTypeIfNeeded()

	return nil
}

func (v *View) initRelations(ctx context.Context, resource *Resource) error {
	var err error
	if len(v.With) == 0 {
		return nil
	}

	for i := range v.With {

		err = v.With[i].Init(ctx, resource)
		if err != nil {
			return err
		}

	}
	return nil
}

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
	v.createColumnMapIfNeeded()

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

func (v *View) ensureSchema(types Types) {
	if v.Schema == nil {
		v.Schema = &Schema{
			Name: v.Name,
		}
	}

	if v.Schema.Name != "" {
		componentType := types.Lookup(v.Schema.Name)
		if componentType != nil {
			v.Schema.setType(componentType)
		}
	}

	v.Schema.Init(v.Columns, v.With, v.Caser)
}

func (v *View) createColumnMapIfNeeded() {
	if v._columns != nil {
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

	if v.Schema == nil && len(v.With) == 0 {
		v.Schema = view.Schema
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

	if v.Parameters == nil {
		v.Parameters = view.Parameters
	}

	if v.MatchStrategy == "" {
		v.MatchStrategy = view.MatchStrategy
	}

	if v.BatchReadSize == nil {
		v.BatchReadSize = view.BatchReadSize
	}

	if v.Selector == nil {
		v.Selector = view.Selector
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
	relations := RelationsSlice(v.With).PopulateWithResolve()
	resolverColumns := RelationsSlice(relations).Columns()

	v.newCollector = func(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector {
		if allowUnmapped {
			return NewCollector(nil, v.Schema.slice, v, dest, supportParallel)
		}
		return NewCollector(resolverColumns, v.Schema.slice, v, dest, supportParallel)
	}
}

func (v *View) Collector(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector {
	return v.newCollector(allowUnmapped, dest, supportParallel)
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

	if len(v.Parameters) > 0 {
		for _, param := range v.Parameters {
			if param.view != nil {
				counter += param.view.DestCount()
			}
		}
	}

	return counter
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

func (v *View) initParams(ctx context.Context, resource *Resource) error {
	if v.Criteria == nil {
		return nil
	}

	for _, param := range v.Parameters {
		if err := param.Init(ctx, resource); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) LimitWithSelector(selector *Selector) int {
	if selector != nil && selector.Limit > 0 {
		return selector.Limit
	}
	return v.Selector.Limit
}

func (v *View) propagateTypeIfNeeded() {
	if v.Schema.AutoGen() {
		return
	}

	for _, childView := range v.With {
		childView.Of.Schema.inheritType(childView.holderField.Type)
	}
}