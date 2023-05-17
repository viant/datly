package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
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
	SQLExecMode  = "SQLExec"
	SQLQueryMode = "SQLQuery"
)

type (
	Mode string

	//View represents a view View
	View struct {
		shared.Reference
		Mode Mode `json:",omitempty"`

		Connector  *Connector `json:",omitempty"`
		Standalone bool       `json:",omitempty"`
		Name       string     `json:",omitempty"`
		Alias      string     `json:",omitempty"`
		Table      string     `json:",omitempty"`
		From       string     `json:",omitempty"`
		FromURL    string     `json:",omitempty"`

		Exclude              []string             `json:",omitempty"`
		Columns              []*Column            `json:",omitempty"`
		InheritSchemaColumns bool                 `json:",omitempty"`
		CaseFormat           formatter.CaseFormat `json:",omitempty"`

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

		DiscoverCriteria *bool  `json:",omitempty"`
		AllowNulls       *bool  `json:",omitempty"`
		Cache            *Cache `json:",omitempty"`

		ColumnsConfig map[string]*ColumnConfig `json:",omitempty"`
		SelfReference *SelfReference           `json:",omitempty"`
		Namespaces    []*Namespace             `json:",omitempty"`
		TableBatches  map[string]bool          `json:",omitempty"`
		Qualifiers    []*Qualifier             `json:",omitempty"`

		_initialized  bool
		_newCollector newCollectorFn
		_codec        *columnsCodec
		_columns      ColumnIndex
		_excluded     map[string]bool
	}

	ViewOption func(v *View)

	ViewOptions []ViewOption

	SelfReference struct {
		Holder string
		Parent string
		Child  string
	}

	newCollectorFn    func(dest interface{}, viewMetaHandler viewMetaHandlerFn, supportParallel bool) *Collector
	viewMetaHandlerFn func(viewMeta interface{}) error

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
		Page        *bool
	}

	Batch struct {
		Parent int `json:",omitempty"`
	}

	Method struct {
		Name string    `json:",omitempty"`
		Args []*Schema `json:",omitempty"`
	}

	Namespace struct {
		Parent string
		Prefix string
		Holder string
	}
)

func (v *View) ViewName() string {
	return v.Name
}

func (v *View) TableAlias() string {
	return v.Alias
}

func (v *View) LookupRelation(name string) (*Relation, error) {
	if len(v.With) == 0 {
		return nil, fmt.Errorf("relation was empty")
	}
	for i, candidate := range v.With {
		if candidate.Name == name || candidate.Of.Name == name {
			return v.With[i], nil
		}
	}
	return nil, fmt.Errorf("failed to lookup relation: %v", name)
}

func (v *View) TableName() string {
	return v.Table
}

func (v *View) ResultLimit() int {
	return v.Selector.Limit
}

func (m *Method) init(resource *Resource) error {
	if m.Name == "" {
		return fmt.Errorf("method name can't be empty")
	}

	for _, arg := range m.Args {
		//TODO: Check format
		if err := arg.Init(nil, nil, format.CaseUpperCamel, resource, nil); err != nil {
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

func (c *Constraints) IsPageEnabled() bool {
	return (c.Limit || c.Offset) || (c.Page != nil && !*c.Page)
}

func (c *Constraints) SqlMethodsIndexed() map[string]*Method {
	return c._sqlMethods
}

// DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Schema.Type()
}

// Init initializes View using view provided in Resource.
// i.e. If View, Connector etc. should inherit from others - it has te bo included in Resource.
// It is important to call Init for every View because it also initializes due to the optimization reasons.
func (v *View) Init(ctx context.Context, resource *Resource, options ...interface{}) error {
	if v._initialized {
		return nil
	}
	v._initialized = true

	var transforms marshal.Transforms
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case marshal.Transforms:
			transforms = actual
		}
	}

	nameTaken := map[string]bool{
		v.Name: true,
	}

	if schema := v.Schema; schema != nil && len(v.With) == 0 {

		if err := v.inheritRelationsFromTag(schema, resource); err != nil {
			return err
		}

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

func (v *View) inheritRelationsFromTag(schema *Schema, resource *Resource) error {
	sType := schema.Type()
	if sType == nil {
		sType, _ = types.GetOrParseType(resource.LookupType, schema.DataType)
		if sType == nil {
			return nil
		}
	}
	recType := getStruct(sType)
	if recType == nil {
		return nil
	}
	for i := 0; i < recType.NumField(); i++ {
		field := recType.Field(i)
		rawTag, ok := field.Tag.Lookup(DatlyTag)
		if !ok {
			continue
		}
		sqlTag, _ := field.Tag.Lookup("sql")
		tag := ParseTag(rawTag, sqlTag)
		if !tag.HasRelationSpec() {
			continue
		}
		refViewOptions, err := v.buildRefViewOptions(tag, resource)
		if err != nil {
			return err
		}
		if viewOptions := tag.RelationOption(field, refViewOptions...); len(viewOptions) > 0 {
			viewOptions.Apply(v)
		}
	}
	return nil
}

func (v *View) buildRefViewOptions(tag *Tag, resource *Resource) ([]ViewOption, error) {
	var refViewOptions []ViewOption
	var err error
	var connector *Connector
	if tag.RefConnector != "" {
		if connector, err = resource.Connector(tag.RefConnector); err != nil {
			return nil, fmt.Errorf("%w, ref view '%v' connector: '%v'", err, tag.RefName, tag.RefConnector)
		}
	} else if v.Connector != nil {
		connector = v.Connector
	}
	if connector != nil {
		refViewOptions = append(refViewOptions, WithConnector(connector))
	}
	return refViewOptions, nil
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

	v.Alias = FirstNotEmpty(v.Alias, "t")
	if v.From == "" {
		v.Table = FirstNotEmpty(v.Table, v.Name)
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

	if err = Columns(v.Columns).Init(resource, v.ColumnsConfig, v.Caser, v.AreNullValuesAllowed()); err != nil {
		return err
	}

	v._columns = Columns(v.Columns).Index(v.Caser)

	if err = v.validateSelfRef(); err != nil {
		return err
	}

	if err = v.ensureSchema(resource); err != nil {
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

	v._codec, err = newColumnsCodec(v.Schema.Type(), v.Columns)
	if err != nil {
		return err
	}

	if v.TableBatches == nil {
		v.TableBatches = map[string]bool{}
	}

	if err = v.initQualifiersIfNeeded(ctx, resource); err != nil {
		return err
	}

	return nil
}

func (v *View) ensureConnector(ctx context.Context, resource *Resource) error {
	if v.Connector != nil && v.Connector._initialized {
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

// ColumnByName returns Column by Column.Name
func (v *View) ColumnByName(name string) (*Column, bool) {
	if column, ok := v._columns[name]; ok {
		return column, true
	}

	return nil, false
}

// Source returns database view source. It prioritizes From, Table then View.Name
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

func (v *View) ensureSchema(resource *Resource) error {
	v.initSchemaIfNeeded()
	if v.Schema.Name != "" {
		componentType, err := resource.LookupType("", "", v.Schema.Name)
		if err != nil {
			return err
		}

		if componentType != nil {
			v.Schema.SetType(componentType)
		}
	}

	return v.Schema.Init(v.Columns, v.With, v.Caser, resource, v.SelfReference)
}

// Db returns database connection that View was assigned to.
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

	v.Alias = FirstNotEmpty(v.Alias, view.Alias)
	v.Table = FirstNotEmpty(v.Table, view.Table)
	v.From = FirstNotEmpty(v.From, view.From)
	v.FromURL = FirstNotEmpty(v.FromURL, view.FromURL)
	v.Mode = Mode(FirstNotEmpty(string(v.Mode), string(view.Mode)))

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

	if v._newCollector == nil && len(v.With) == 0 {
		v._newCollector = view._newCollector
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
		v.Selector = view.Selector.Clone()
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

	if v.SelfReference == nil {
		v.SelfReference = view.SelfReference
	}

	if v.TableBatches == nil {
		v.TableBatches = view.TableBatches
	}

	if len(v.Qualifiers) == 0 {
		v.Qualifiers = view.Qualifiers
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

		v.CaseFormat = formatter.CaseFormat(formatter.DetectCase(columnNames...))
	}

	if err := v.CaseFormat.Init(); err != nil {
		return err
	}

	var err error
	v.Caser, err = v.CaseFormat.Caser()
	return err
}

func (v *View) ensureCollector() {
	v._newCollector = func(dest interface{}, viewMetaHandler viewMetaHandlerFn, supportParallel bool) *Collector {
		return NewCollector(v.Schema.slice, v, dest, viewMetaHandler, supportParallel)
	}
}

// Collector creates new Collector for View.DataType
func (v *View) Collector(dest interface{}, handleMeta viewMetaHandlerFn, supportParallel bool) *Collector {
	return v._newCollector(dest, handleMeta, supportParallel)
}

func FirstNotEmpty(values ...string) string {
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

// CanUseSelectorCriteria indicates if Selector.Criteria can be used
func (v *View) CanUseSelectorCriteria() bool {
	return v.Selector.Constraints.Criteria
}

// CanUseSelectorLimit indicates if Selector.Limit can be used
func (v *View) CanUseSelectorLimit() bool {
	return v.Selector.Constraints.Limit
}

// CanUseSelectorOrderBy indicates if Selector.OrderBy can be used
func (v *View) CanUseSelectorOrderBy() bool {
	return v.Selector.Constraints.OrderBy
}

// CanUseSelectorOffset indicates if Selector.Offset can be used
func (v *View) CanUseSelectorOffset() bool {
	return v.Selector.Constraints.Offset
}

// CanUseSelectorProjection indicates if Selector.Fields can be used
func (v *View) CanUseSelectorProjection() bool {
	return v.Selector.Constraints.Projection
}

// IndexedColumns returns Columns
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
				continue
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
	columnsIndex := Columns(newColumns).Index(v.Caser)

	if err := v.updateColumn("", shared.Elem(v.Schema.Type()), &newColumns, relation, columnsIndex); err != nil {
		return err
	}

	v.Columns = newColumns
	v._columns = Columns(newColumns).Index(v.Caser)

	return nil
}

func (v *View) updateColumn(ns string, rType reflect.Type, columns *[]*Column, relation *Relation, columnsIndex ColumnIndex) error {
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		isExported := field.PkgPath == ""
		if !isExported {
			continue
		}

		if field.Anonymous {
			if err := v.updateColumn("", field.Type, columns, relation, columnsIndex); err != nil {
				return err
			}
			continue
		}

		sqlxTag := io.ParseTag(field.Tag.Get(option.TagSqlx))
		elemType := types.Elem(field.Type)
		if !v.IsHolder(field.Name) && sqlxTag.Ns != "" && elemType.Kind() == reflect.Struct {
			if err := v.updateColumn(sqlxTag.Ns, elemType, columns, relation, columnsIndex); err != nil {
				return err
			}
			continue
		}

		fieldName := sqlxTag.Column
		if fieldName == "" {
			fieldName = field.Name
		}

		fieldName = ns + fieldName
		if _, ok := columnsIndex[fieldName]; ok {
			continue
		}

		column, ok := v.findSchemaColumn(fieldName)
		if ok {
			*columns = append(*columns, column)
			//			column.field = &field
			columnsIndex.Register(v.Caser, column)
		}
	}

	for _, rel := range v.With {
		if _, ok := columnsIndex[rel.Of.Column]; ok {
			continue
		}

		col, err := v._columns.Lookup(rel.Column)
		if err != nil {
			return fmt.Errorf("invalid rel: %v %w", rel.Name, err)
		}

		*columns = append(*columns, col)
	}

	if relation != nil {
		_, err := columnsIndex.Lookup(relation.Of.Column)
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
	if v._codec != nil {
		return v._codec.actualType
	}

	return v.Schema.Type()
}

func (v *View) UnwrapDatabaseType(ctx context.Context, value interface{}) (interface{}, error) {
	if v._codec != nil {
		actualRecord := v._codec.unwrapper.Value(xunsafe.AsPointer(value))

		if err := v._codec.updateValue(ctx, value, &config.ParentValue{Value: actualRecord, RType: v.Schema.Type()}); err != nil {
			return nil, err
		}

		actualRecord = v._codec.unwrapper.Value(xunsafe.AsPointer(value))
		return actualRecord, nil
	}

	return value, nil
}

func (v *View) indexTransforms(resource *Resource, transforms marshal.Transforms) error {
	for _, transform := range transforms {
		if strings.Contains(transform.Path, ".") {
			continue
		}

		columnName := format.CaseUpperCamel.Format(transform.Path, v.Caser)
		aConfig, ok := v.ColumnsConfig[columnName]
		if !ok {
			aConfig = &ColumnConfig{}
			v.ColumnsConfig[columnName] = aConfig
		}

		visitor, ok := resource.CodecByName(transform.Codec)
		if !ok {
			return fmt.Errorf("not found codec %v", transform.Codec)
		}

		actualCodec, ok := visitor.(config.CodecDef)
		if !ok {
			return fmt.Errorf("expected %v codec to be type of %T but was %T", transform.Codec, actualCodec, visitor)
		}

		resultType, err := actualCodec.ResultType(nil)
		if err != nil {
			return err
		}
		aConfig.Codec = &Codec{
			Name:     transform.Codec,
			Schema:   NewSchema(resultType),
			_codecFn: actualCodec.Valuer().Value,
		}
	}

	return nil
}

func (v *View) Expand(placeholders *[]interface{}, SQL string, selector *Selector, params CriteriaParam, batchData *BatchData, sanitized *expand.DataUnit) (string, error) {
	v.ensureParameters(selector)

	return v.Template.Expand(placeholders, SQL, selector, params, batchData, sanitized)
}

func (v *View) ensureParameters(selector *Selector) {
	if v.Template == nil {
		return
	}

	if selector.Parameters.Values == nil {
		selector.Parameters.Values = expand.NewValue(v.Template.Schema.Type())
	}

	if selector.Parameters.Has == nil {
		selector.Parameters.Has = expand.NewValue(v.Template.PresenceSchema.Type())
	}
}

func (v *View) ParamByName(name string) (*Parameter, error) {
	return v.Template._parametersIndex.Lookup(name)
}

func (v *View) MetaTemplateEnabled() bool {
	return v.Template.Meta != nil
}

func (v *View) AreNullValuesAllowed() bool {
	return v.AllowNulls != nil && !*v.AllowNulls
}

func (v *View) validateSelfRef() error {
	if v.SelfReference == nil {
		return nil
	}

	if v.SelfReference.Holder == "" {
		return fmt.Errorf("view %v SelfReference Holder can't be empty", v.Name)
	}

	if v.SelfReference.Child == "" {
		return fmt.Errorf("view %v SelfReference Child can't be empty", v.Name)
	}

	if _, err := v._columns.Lookup(v.SelfReference.Child); err != nil {
		return err
	}

	if v.SelfReference.Parent == "" {
		return fmt.Errorf("view %v SelfReference Parent can't be empty", v.Name)
	}

	if _, err := v._columns.Lookup(v.SelfReference.Parent); err != nil {
		return err
	}

	return nil
}

func (v *View) findSchemaColumn(fieldName string) (*Column, bool) {

	lookup, err := v._columns.Lookup(fieldName)
	return lookup, err == nil
}

//SetParameter sets a view or relation parameter, for relation name has to be prefixed relName:paramName
func (v *View) SetParameter(name string, selectors *Selectors, value interface{}) error {
	aView := v
	if strings.Contains(name, ":") {
		pair := strings.SplitN(name, ":", 2)
		name = pair[1]
		relation, err := v.LookupRelation(pair[0])
		if err != nil {
			return err
		}
		aView = &relation.Of.View
	}
	param, err := aView.ParamByName(name)
	if err != nil {
		return err
	}
	selector := selectors.Lookup(aView)
	if selector == nil {
		return fmt.Errorf("failed to lookup selector: %v", aView.Name)
	}
	return param.Set(selector, value)
}

func (v *View) initQualifiersIfNeeded(ctx context.Context, resource *Resource) error {
	for _, qualifier := range v.Qualifiers {
		if err := qualifier.Init(ctx, resource, v, v._columns); err != nil {
			return err
		}
	}

	return nil
}

//WithSQL creates SQL FROM view option
func WithSQL(SQL string) ViewOption {
	return func(v *View) {
		if v.Template == nil {
			v.Template = &Template{}
		}
		v.Template.Source = SQL
	}
}

//WithConnector creates connector view option
func WithConnector(connector *Connector) ViewOption {
	return func(v *View) {
		v.Connector = connector
	}
}

//WithTemplate creates connector view option
func WithTemplate(template *Template) ViewOption {
	return func(v *View) {

		v.Template = template
	}
}

//WithOneToMany creates to many relation view option
func WithOneToMany(holder, column string, ref *ReferenceView) ViewOption {
	return func(v *View) {
		relation := &Relation{Cardinality: Many, Column: column, Holder: holder, Of: ref}
		v.With = append(v.With, relation)
	}
}

//WithOneToOne creates to one relation view option
func WithOneToOne(holder, column string, ref *ReferenceView) ViewOption {
	return func(v *View) {
		relation := &Relation{Cardinality: One, Column: column, Holder: holder, Of: ref}
		v.With = append(v.With, relation)
	}
}

//WithCriteria creates criteria constraints view option
func WithCriteria(columns ...string) ViewOption {
	return func(v *View) {
		if v.Selector == nil {
			v.Selector = &Config{}
		}
		if v.Selector.Constraints == nil {
			v.Selector.Constraints = &Constraints{}
		}
		v.Selector.Constraints.Criteria = true
		v.Selector.Constraints.Filterable = columns
	}
}

//WithViewType creates schema type view option
func WithViewType(t reflect.Type) ViewOption {
	return func(v *View) {
		v.Schema = NewSchema(t)
	}
}

func WithViewKind(mode Mode) ViewOption {
	return func(aView *View) {
		aView.Mode = mode
	}
}

func (o ViewOptions) Apply(view *View) {
	if len(o) == 0 {
		return
	}
	for _, opt := range o {
		opt(view)
	}
}

//NewView creates a view
func NewView(name, table string, opts ...ViewOption) *View {
	ret := &View{Name: name, Table: table}
	ViewOptions(opts).Apply(ret)
	return ret
}

//NewExecView creates an execution view
func NewExecView(name, table string, template string, parameters []*Parameter, opts ...ViewOption) *View {
	var templateParameters []TemplateOption
	for i := range parameters {
		templateParameters = append(templateParameters, WithTemplateParameter(parameters[i]))
	}
	opts = append(opts, WithViewKind(SQLExecMode),
		WithTemplate(NewTemplate(template, templateParameters...)))
	return NewView(name, table, opts...)
}
