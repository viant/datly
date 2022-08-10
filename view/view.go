package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/gmetric/provider"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"

	"reflect"
	"strings"
	"time"
)

const (
	WriteMode = "Write"
	ReadMode  = "Read"
)

type (
	Mode string

	//View represents a view View
	View struct {
		shared.Reference
		Mode Mode

		Connector  *Connector `json:",omitempty"`
		Standalone bool       `json:",omitempty"`
		Name       string     `json:",omitempty"`
		Alias      string     `json:",omitempty"`
		Table      string     `json:",omitempty"`
		From       string     `json:",omitempty"`
		FromURL    string     `json:",omitempty"`

		Exclude              []string   `json:",omitempty"`
		Columns              []*Column  `json:",omitempty"`
		InheritSchemaColumns bool       `json:",omitempty"`
		CaseFormat           CaseFormat `json:",omitempty"`

		Criteria string `json:",omitempty"`

		Selector *Config   `json:",omitempty"`
		Template *Template `json:",omitempty"`

		Schema *Schema `json:",omitempty"`

		With []*Relation `json:",omitempty"`

		MatchStrategy MatchStrategy `json:",omitempty"`
		Batch         *Batch        `json:",omitempty"`

		Logger  *logger.Adapter `json:",omitempty"`
		Counter logger.Counter  `json:"-"`
		Caser   format.Case     `json:",omitempty"`

		_columns  ColumnIndex
		_excluded map[string]bool

		DiscoverCriteria *bool  `json:",omitempty"`
		AllowNulls       *bool  `json:",omitempty"`
		Cache            *Cache `json:",omitempty"`

		ColumnsConfig map[string]*ColumnConfig `json:",omitempty"`

		initialized  bool
		newCollector func(dest interface{}, supportParallel bool) *Collector

		codec *columnsCodec
	}

	//Constraints configure what can be selected by Selector
	//For each _field, default value is `false`
	Constraints struct {
		Criteria    bool
		OrderBy     bool
		Limit       bool
		Offset      bool
		Projection  bool //enables columns projection from client (default ${NS}_fields= query param)
		Filterable  []string
		SQLMethods  []*Method `json:",omitempty"`
		_sqlMethods map[string]*Method
	}

	Batch struct {
		Parent int `json:",omitempty"`
	}

	Method struct {
		Name string    `json:",omitempty"`
		Args []*Schema `json:",omitempty"`
	}
)

func (m *Method) init(resource *Resource) error {
	if m.Name == "" {
		return fmt.Errorf("method name can't be empty")
	}

	for _, arg := range m.Args {
		//TODO: Check format
		if err := arg.Init(nil, nil, format.CaseUpperCamel, resource.GetTypes()); err != nil {
			return err
		}
	}

	return nil
}

func (c *Constraints) init(resource *Resource) error {
	c._sqlMethods = map[string]*Method{}
	for i, method := range c.SQLMethods {
		c._sqlMethods[method.Name] = c.SQLMethods[i]
	}

	for _, method := range c.SQLMethods {
		if err := method.init(resource); err != nil {
			return err
		}
	}

	return nil
}

func (c *Constraints) SqlMethodsIndexed() map[string]*Method {
	return c._sqlMethods
}

//DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Schema.Type()
}

//Init initializes View using view provided in Resource.
//i.e. If View, Connector etc. should inherit from others - it has te bo included in Resource.
//It is important to call Init for every View because it also initializes due to the optimization reasons.
func (v *View) Init(ctx context.Context, resource *Resource, options ...interface{}) error {
	if v.initialized {
		return nil
	}

	var transforms marshal.Transforms
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case marshal.Transforms:
			transforms = actual
		}
	}

	v.initialized = true

	nameTaken := map[string]bool{
		v.Name: true,
	}

	if err := v.initViews(ctx, resource, v.With, nameTaken, transforms); err != nil {
		return err
	}

	if err := v.initView(ctx, resource, transforms); err != nil {
		return err
	}

	if err := v.updateRelations(ctx, resource, v.With); err != nil {
		return err
	}

	return nil
}

func (v *View) loadFromWithURL(ctx context.Context, resource *Resource) error {
	if v.FromURL == "" || v.From != "" {
		return nil
	}
	var err error
	v.From, err = resource.LoadText(ctx, v.FromURL)
	return err
}

func (v *View) initViews(ctx context.Context, resource *Resource, relations []*Relation, notUnique map[string]bool, transforms marshal.Transforms) error {
	for _, rel := range relations {
		refView := &rel.Of.View
		v.generateNameIfNeeded(refView, rel)
		isNotUnique := notUnique[rel.Of.View.Name]
		if isNotUnique {
			return fmt.Errorf("not unique view name: %v", rel.Of.View.Name)
		}
		notUnique[rel.Of.View.Name] = true
		relTransforms := marshal.Transforms{}
		for _, transform := range transforms {
			pathPrefix := rel.Holder + "."
			if strings.HasPrefix(transform.Path, pathPrefix) {
				relTransform := *transform

				relTransform.Path = relTransform.Path[len(pathPrefix):]
				relTransforms = append(relTransforms, &relTransform)
			}
		}

		if err := refView.inheritFromViewIfNeeded(ctx, resource, relTransforms); err != nil {
			return err
		}

		if err := rel.BeforeViewInit(ctx); err != nil {
			return err
		}

		if err := refView.initViews(ctx, resource, refView.With, notUnique, relTransforms); err != nil {
			return err
		}

		if err := refView.initView(ctx, resource, relTransforms); err != nil {
			return err
		}

	}
	return nil
}

func (v *View) generateNameIfNeeded(refView *View, rel *Relation) {
	if refView.Name == "" {
		refView.Name = v.Name + "#rel:" + rel.Name
	}
}

func (v *View) initView(ctx context.Context, resource *Resource, transforms marshal.Transforms) error {
	var err error
	if err = v.loadFromWithURL(ctx, resource); err != nil {
		return err
	}

	if err = v.inheritFromViewIfNeeded(ctx, resource, transforms); err != nil {
		return err
	}
	if v.ColumnsConfig == nil {
		v.ColumnsConfig = map[string]*ColumnConfig{}
	}

	v.ensureIndexExcluded()
	v.ensureBatch()

	if err = v.ensureLogger(resource); err != nil {
		return err
	}

	v.ensureCounter(resource)

	v.Alias = notEmptyOf(v.Alias, "t")
	if v.From == "" {
		v.Table = notEmptyOf(v.Table, v.Name)
	} else {
		if strings.Contains(v.From, keywords.WhereCriteria) {
			flag := false
			v.DiscoverCriteria = &flag
		}
	}

	if v.MatchStrategy == "" {
		v.MatchStrategy = ReadMatched
	}

	if err = v.MatchStrategy.Validate(); err != nil {
		return err
	}

	if v.Selector == nil {
		v.Selector = &Config{}
	}

	if v.Name == v.Ref && !v.Standalone {
		return fmt.Errorf("view name and ref cannot be the same")
	}

	if v.Name == "" {
		return fmt.Errorf("view name was empty")
	}

	if err = v.ensureConnector(ctx, resource); err != nil {
		return err
	}

	if err = v.ensureColumns(ctx, resource); err != nil {
		return err
	}

	if err = v.ensureCaseFormat(); err != nil {
		return err
	}

	if err = v.indexTransforms(resource, transforms); err != nil {
		return err
	}

	if err = Columns(v.Columns).Init(resource, v.ColumnsConfig, v.Caser, v.AllowNulls != nil && !*v.AllowNulls); err != nil {
		return err
	}

	v._columns = Columns(v.Columns).Index(v.Caser)

	if err = v.ensureSchema(resource._types); err != nil {
		return err
	}

	if err = v.Selector.Init(ctx, resource, v); err != nil {
		return err
	}

	if err = v.markColumnsAsFilterable(); err != nil {
		return err
	}

	v.updateColumnTypes()

	if err = v.initTemplate(ctx, resource); err != nil {
		return err
	}

	if v.Cache != nil {
		if err = v.Cache.init(ctx, resource, v); err != nil {
			return err
		}
	}

	v.codec, err = newColumnsCodec(v.Schema.Type(), v.Columns)
	if err != nil {
		return err
	}

	return nil
}

func (v *View) ensureConnector(ctx context.Context, resource *Resource) error {
	if v.Connector != nil && v.Connector.initialized {
		return nil
	}

	var err error
	if v.Connector, err = resource.FindConnector(v); err != nil {
		return err
	}

	if err = v.Connector.Init(ctx, resource._connectors); err != nil {
		return err
	}

	if err = v.Connector.Validate(); err != nil {
		return err
	}
	return nil
}

func (v *View) ensureCounter(resource *Resource) {
	if v.Counter != nil {
		return
	}
	var counter logger.Counter
	if metric := resource.Metrics; metric != nil {
		name := v.Name
		if metric.URIPart != "" {
			name = metric.URIPart + name
		}
		name = strings.ReplaceAll(name, "/", ".")

		cnt := metric.Service.LookupOperation(name)
		if cnt == nil {
			counter = metric.Service.MultiOperationCounter(metricLocation(), name, name+" performance", time.Millisecond, time.Minute, 2, provider.NewBasic())
		} else {
			counter = cnt
		}
	}

	v.Counter = logger.NewCounter(counter)

}

func (v *View) updateColumnTypes() {
	rType := shared.Elem(v.DataType())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)

		column, err := v._columns.Lookup(field.Name)
		if err != nil {
			continue
		}

		column.setField(field)
	}
}

func (v *View) updateRelations(ctx context.Context, resource *Resource, relations []*Relation) error {
	v.indexColumns()
	if err := v.indexSqlxColumnsByFieldName(); err != nil {
		return err
	}

	v.ensureCollector()

	if err := v.deriveColumnsFromSchema(nil); err != nil {
		return err
	}

	for _, rel := range relations {
		if err := rel.Init(ctx, resource, v); err != nil {
			return err
		}

		refView := rel.Of.View
		if err := refView.updateRelations(ctx, resource, refView.With); err != nil {
			return err
		}
	}

	if err := v.registerHolders(); err != nil {
		return err
	}

	return nil
}

func (v *View) ensureColumns(ctx context.Context, resource *Resource) error {
	if resource._columnsCache != nil {
		if cachedColumns, ok := resource._columnsCache[v.Name]; ok {
			v.Columns = cachedColumns
		}
	}

	if len(v.Columns) != 0 {
		return nil
	}

	columns, SQL, err := DetectColumns(ctx, resource, v)
	v.Logger.ColumnsDetection(SQL, v.Source())

	if err != nil {
		return fmt.Errorf("failed to run query: %v due to %w", SQL, err)
	}

	v.Columns = columns
	if resource._columnsCache != nil {
		resource._columnsCache[v.Name] = v.Columns
	}
	return nil
}

func convertIoColumnsToColumns(ioColumns []io.Column, nullable map[string]bool) []*Column {
	columns := make([]*Column, 0)
	for i := 0; i < len(ioColumns); i++ {
		scanType := ioColumns[i].ScanType()
		scanType = remapScanType(scanType, ioColumns[i].DatabaseTypeName())
		dataTypeName := ioColumns[i].DatabaseTypeName()
		isNullable, _ := ioColumns[i].Nullable()
		columns = append(columns, &Column{
			DatabaseColumn: ioColumns[i].Name(),
			Name:           strings.Trim(ioColumns[i].Name(), "'"),
			DataType:       dataTypeName,
			rType:          scanType,
			Nullable:       nullable[ioColumns[i].Name()] || isNullable,
		})
	}
	return columns
}

var (
	sqlNullInt64Type   = reflect.TypeOf(sql.NullInt64{})
	sqlNullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
	sqlRawBytesType    = reflect.TypeOf(sql.RawBytes{})
	sqlNullTimeType    = reflect.TypeOf(sql.NullTime{})
)

func remapScanType(scanType reflect.Type, name string) reflect.Type {
	switch scanType {
	case sqlNullInt64Type:
		v := int64(0)
		scanType = reflect.TypeOf(&v)
	case sqlNullTimeType:
		t := time.Time{}
		scanType = reflect.TypeOf(&t)
	case sqlNullFloat64Type:
		f := float64(0)
		scanType = reflect.TypeOf(&f)
	case sqlRawBytesType:
		switch name {
		case "BIT":
			scanType = reflect.TypeOf([]byte{})
		default:
			scanType = reflect.TypeOf("")
		}
	}
	return scanType
}

//ColumnByName returns Column by Column.Name
func (v *View) ColumnByName(name string) (*Column, bool) {
	if column, ok := v._columns[name]; ok {
		return column, true
	}

	return nil, false
}

//Source returns database view source. It prioritizes From, Table then View.Name
func (v *View) Source() string {
	if v.From != "" {
		if v.From[0] == '(' {
			return v.From
		}
		return "(" + v.From + ")"
	}

	if v.Table != "" {
		return v.Table
	}

	return v.Name
}

func (v *View) ensureSchema(types Types) error {
	v.initSchemaIfNeeded()
	if v.Schema.Name != "" {
		componentType, err := types.Lookup(v.Schema.Name)
		if err != nil {
			return err
		}

		if componentType != nil {
			v.Schema.setType(componentType)
		}
	}

	return v.Schema.Init(v.Columns, v.With, v.Caser, types)
}

//Db returns database connection that View was assigned to.
func (v *View) Db() (*sql.DB, error) {
	return v.Connector.DB()
}

func (v *View) exclude(columns []io.Column) []io.Column {
	if len(v.Exclude) == 0 {
		return columns
	}

	filtered := make([]io.Column, 0, len(columns))
	for i := range columns {
		if _, ok := v._excluded[columns[i].Name()]; ok {
			continue
		}
		filtered = append(filtered, columns[i])
	}
	return filtered
}

func (v *View) inherit(view *View) error {
	if v.Connector == nil {
		v.Connector = view.Connector
	}

	v.Alias = notEmptyOf(v.Alias, view.Alias)
	v.Table = notEmptyOf(v.Table, view.Table)
	v.From = notEmptyOf(v.From, view.From)
	v.FromURL = notEmptyOf(v.FromURL, view.FromURL)
	v.Mode = Mode(notEmptyOf(string(v.Mode), string(view.Mode)))

	if stringsSliceEqual(v.Exclude, view.Exclude) {
		if len(v.Columns) == 0 {
			v.Columns = view.Columns
		}

		if v.Schema == nil && len(v.With) == 0 {
			v.Schema = view.Schema
		}
	}

	if len(v.With) == 0 {
		v.With = view.With
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

	if v.Template == nil && view.Template != nil {
		tempCopy := *view.Template
		tempCopy._view = v
		v.Template = &tempCopy
	}

	if v.MatchStrategy == "" {
		v.MatchStrategy = view.MatchStrategy
	}

	if v.Selector == nil && view.Selector != nil {
		v.Selector = view.Selector
	}

	if v.Logger == nil {
		v.Logger = view.Logger
	}

	if v.Batch == nil {
		v.Batch = view.Batch
	}

	if v.AllowNulls == nil {
		v.AllowNulls = view.AllowNulls
	}

	if v.Cache == nil && view.Cache != nil {
		shallowCopy := *view.Cache
		v.Cache = &shallowCopy
	}

	if v.ColumnsConfig == nil {
		v.ColumnsConfig = view.ColumnsConfig
	}

	return nil
}

func stringsSliceEqual(x []string, y []string) bool {
	if len(x) != len(y) {
		return false
	}

	for index, value := range x {
		if y[index] != value {
			return false
		}
	}

	return true
}

func (v *View) ensureIndexExcluded() {
	if len(v.Exclude) == 0 {
		return
	}

	v._excluded = Names(v.Exclude).Index()
}

func (v *View) ensureCaseFormat() error {
	if v.CaseFormat == "" && len(v.Columns) > 0 {
		columnNames := make([]string, len(v.Columns))
		for i, column := range v.Columns {
			columnNames[i] = column.Name
		}

		v.CaseFormat = CaseFormat(DetectCase(columnNames...))
	}

	if err := v.CaseFormat.Init(); err != nil {
		return err
	}

	var err error
	v.Caser, err = v.CaseFormat.Caser()
	return err
}

func (v *View) ensureCollector() {
	v.newCollector = func(dest interface{}, supportParallel bool) *Collector {
		return NewCollector(v.Schema.slice, v, dest, supportParallel)
	}
}

//Collector creates new Collector for View.DataType
func (v *View) Collector(dest interface{}, supportParallel bool) *Collector {
	return v.newCollector(dest, supportParallel)
}

func notEmptyOf(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func (v *View) registerHolders() error {
	for i := range v.With {
		if err := v._columns.RegisterHolder(v.With[i]); err != nil {
			return err
		}
	}

	return nil
}

//CanUseSelectorCriteria indicates if Selector.Criteria can be used
func (v *View) CanUseSelectorCriteria() bool {
	return v.Selector.Constraints.Criteria
}

//CanUseSelectorLimit indicates if Selector.Limit can be used
func (v *View) CanUseSelectorLimit() bool {
	return v.Selector.Constraints.Limit
}

//CanUseSelectorOrderBy indicates if Selector.OrderBy can be used
func (v *View) CanUseSelectorOrderBy() bool {
	return v.Selector.Constraints.OrderBy
}

//CanUseSelectorOffset indicates if Selector.Offset can be used
func (v *View) CanUseSelectorOffset() bool {
	return v.Selector.Constraints.Offset
}

//CanUseSelectorProjection indicates if Selector.Fields can be used
func (v *View) CanUseSelectorProjection() bool {
	return v.Selector.Constraints.Projection
}

//IndexedColumns returns Columns
func (v *View) IndexedColumns() ColumnIndex {
	return v._columns
}

func (v *View) markColumnsAsFilterable() error {
	if len(v.Selector.Constraints.Filterable) == 1 && strings.TrimSpace(v.Selector.Constraints.Filterable[0]) == "*" {
		for _, column := range v.Columns {
			column.Filterable = true
		}

		return nil
	}

	for _, colName := range v.Selector.Constraints.Filterable {
		column, err := v._columns.Lookup(colName)
		if err != nil {
			return fmt.Errorf("invalid view: %v %w", v.Name, err)
		}
		column.Filterable = true
	}
	return nil
}

func (v *View) indexSqlxColumnsByFieldName() error {
	rType := shared.Elem(v.Schema.Type())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		isExported := field.PkgPath == ""
		if !isExported {
			continue
		}

		tag := io.ParseTag(field.Tag.Get(option.TagSqlx))
		//TODO: support anonymous fields
		if tag.Column != "" {
			column, err := v._columns.Lookup(tag.Column)
			if err != nil {
				return fmt.Errorf("invalid view: %v %w", v.Name, err)
			}
			v._columns.RegisterWithName(field.Name, column)
		}
	}

	return nil
}

func (v *View) deriveColumnsFromSchema(relation *Relation) error {
	if !v.InheritSchemaColumns {
		return nil
	}

	newColumns := make([]*Column, 0)

	if err := v.updateColumn(shared.Elem(v.Schema.Type()), &newColumns, relation); err != nil {
		return err
	}

	v.Columns = newColumns
	v._columns = Columns(newColumns).Index(v.Caser)

	return nil
}

func (v *View) updateColumn(rType reflect.Type, columns *[]*Column, relation *Relation) error {
	index := Columns(*columns).Index(v.Caser)

	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		isExported := field.PkgPath == ""
		if !isExported {
			continue
		}

		if field.Anonymous {
			if err := v.updateColumn(field.Type, columns, relation); err != nil {
				return err
			}
			continue
		}

		if _, ok := index[field.Name]; ok {
			continue
		}

		column, err := v._columns.Lookup(field.Name)
		if err == nil {
			*columns = append(*columns, column)
		}

		tag := io.ParseTag(field.Tag.Get(option.TagSqlx))
		column, err = v._columns.Lookup(tag.Column)
		if err == nil {
			*columns = append(*columns, column)
			index.Register(v.Caser, column)
		}
	}

	for _, rel := range v.With {
		if _, ok := index[rel.Of.Column]; ok {
			continue
		}

		col, err := v._columns.Lookup(rel.Column)
		if err != nil {
			return fmt.Errorf("invalid rel: %v %w", rel.Name, err)
		}

		*columns = append(*columns, col)
	}

	if relation != nil {
		_, err := index.Lookup(relation.Of.Column)
		if err != nil {
			col, err := v._columns.Lookup(relation.Of.Column)
			if err != nil {
				return fmt.Errorf("invalid ref: %v %w", relation.Name, err)
			}
			*columns = append(*columns, col)
		}
	}

	return nil
}

func (v *View) initSchemaIfNeeded() {
	if v.Schema == nil {
		v.Schema = &Schema{
			autoGen: true,
		}
	}
}

func (v *View) inheritFromViewIfNeeded(ctx context.Context, resource *Resource, transforms marshal.Transforms) error {
	if v.Ref != "" {
		view, err := resource._views.Lookup(v.Ref)
		if err != nil {
			return err
		}

		if err = view.Init(ctx, resource, transforms); err != nil {
			return err
		}

		if err = v.inherit(view); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) indexColumns() {
	v._columns = Columns(v.Columns).Index(v.Caser)
}

func (v *View) ensureLogger(resource *Resource) error {
	if v.Logger == nil {
		v.Logger = logger.Default()
		return nil
	}

	if v.Logger.Ref != "" {
		adapter, ok := resource._loggers.Lookup(v.Logger.Ref)
		if !ok {
			return fmt.Errorf("not found Logger %v in Resource", v.Logger.Ref)
		}

		v.Logger.Inherit(adapter)
	}

	return nil
}

func (v *View) ensureBatch() {
	if v.Batch != nil {
		return
	}

	v.Batch = &Batch{
		Parent: 10000,
	}
}

func (v *View) initTemplate(ctx context.Context, res *Resource) error {
	if v.Template == nil {
		v.Template = &Template{}
	}

	return v.Template.Init(ctx, res, v)
}

func (v *View) IsHolder(value string) bool {
	for _, relation := range v.With {
		if relation.Holder == value {
			return true
		}
	}

	return false
}

func (v *View) ShouldTryDiscover() bool {
	return v.DiscoverCriteria == nil || *v.DiscoverCriteria
}

func (v *View) DatabaseType() reflect.Type {
	if v.codec != nil {
		return v.codec.actualType
	}

	return v.Schema.Type()
}

func (v *View) UnwrapDatabaseType(ctx context.Context, value interface{}) (interface{}, error) {
	if v.codec != nil {
		if err := v.codec.updateValue(ctx, value); err != nil {
			return nil, err
		}

		return v.codec.unwrapper.Value(xunsafe.AsPointer(value)), nil
	}

	return value, nil
}

func (v *View) indexTransforms(resource *Resource, transforms marshal.Transforms) error {
	for _, transform := range transforms {
		if strings.Contains(transform.Path, ".") {
			continue
		}

		columnName := format.CaseUpperCamel.Format(transform.Path, v.Caser)
		config, ok := v.ColumnsConfig[columnName]
		if !ok {
			config = &ColumnConfig{}
			v.ColumnsConfig[columnName] = config
		}

		visitor, ok := resource.VisitorByName(transform.Codec)
		if !ok {
			return fmt.Errorf("not found codec %v", transform.Codec)
		}

		actualCodec, ok := visitor.(codec.Codec)
		if !ok {
			return fmt.Errorf("expected %v codec to be type of %T but was %T", transform.Codec, actualCodec, visitor)
		}

		config.Codec = &Codec{
			Name:     transform.Codec,
			Schema:   NewSchema(actualCodec.ResultType()),
			_codecFn: actualCodec.Valuer().Value,
		}
	}

	return nil
}

func (v *View) Expand(placeholders *[]interface{}, SQL string, selector *Selector, params CommonParams, batchData *BatchData, sanitized *CriteriaSanitizer) (string, error) {
	v.ensureParameters(selector)

	return v.Template.Expand(placeholders, SQL, selector, params, batchData, sanitized)
}

func (v *View) ensureParameters(selector *Selector) {
	if v.Template == nil {
		return
	}

	if selector.Parameters.Values == nil {
		selector.Parameters.Values = newValue(v.Template.Schema.Type())
	}

	if selector.Parameters.Has == nil {
		selector.Parameters.Has = newValue(v.Template.PresenceSchema.Type())
	}
}

func (v *View) ParamByName(name string) (*Parameter, error) {
	return v.Template._parametersIndex.Lookup(name)
}
