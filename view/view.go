package view

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/logger"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/column"
	"github.com/viant/datly/view/extension/codec"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/gmetric/provider"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
	"time"
)

const (
	ModeExec        = Mode("SQLExec")
	ModeQuery       = Mode("SQLQuery")
	ModeUnspecified = Mode("")
	ModeHandler     = Mode("SQLHandler")

	AsyncJobsTable = "DATLY_JOBS"
	AsyncTagName   = "sqlxAsync"
)

type (
	Mode string

	//View represents a View
	View struct {
		shared.Reference
		Mode        Mode       `json:",omitempty"`
		Connector   *Connector `json:",omitempty"`
		Standalone  bool       `json:",omitempty"`
		Name        string     `json:",omitempty"`
		Description string     `json:",omitempty"`
		Module      string     `json:",omitempty"`
		Alias       string     `json:",omitempty"`
		Table       string     `json:",omitempty"`
		From        string     `json:",omitempty"`
		FromURL     string     `json:",omitempty"`
		Exclude     []string   `json:",omitempty"`
		Columns     []*Column  `json:",omitempty"`

		Criteria string `json:",omitempty"`

		Selector *Config   `json:",omitempty"`
		Template *Template `json:",omitempty"`

		Schema *state.Schema `json:",omitempty"`

		With []*Relation `json:",omitempty"`

		MatchStrategy MatchStrategy `json:",omitempty"`
		Batch         *Batch        `json:",omitempty"`

		Logger     *logger.Adapter `json:",omitempty"`
		Counter    logger.Counter  `json:"-"`
		CaseFormat text.CaseFormat `json:",omitempty"`

		DiscoverCriteria *bool  `json:",omitempty"`
		AllowNulls       *bool  `json:",omitempty"`
		Cache            *Cache `json:",omitempty"`

		ColumnsConfig map[string]*ColumnConfig `json:",omitempty"`
		SelfReference *SelfReference           `json:",omitempty"`

		TableBatches     map[string]bool `json:",omitempty"`
		_transforms      marshal.Transforms
		_resource        *Resource
		_fs              *embed.FS
		_initialized     bool
		_newCollector    newCollectorFn
		_codec           *columnsCodec
		_columns         NamedColumns
		_excluded        map[string]bool
		_inputParameters state.Parameters
		_parent          *View
	}

	//contextKey context key
	contextKey string

	SelfReference struct {
		Holder string
		Parent string
		Child  string
	}

	newCollectorFn    func(dest interface{}, viewMetaHandler viewMetaHandlerFn, supportParallel bool) *Collector
	viewMetaHandlerFn func(viewMeta interface{}) error

	Batch struct {
		Parent int `json:",omitempty"`
	}

	Method struct {
		Name string          `json:",omitempty"`
		Args []*state.Schema `json:",omitempty"`
	}
)

// ContextKey view context key
var ContextKey = contextKey("view")

func Context(ctx context.Context) *View {
	if ctx == nil {
		return nil
	}
	value := ctx.Value(ContextKey)
	if value == nil {
		return nil
	}
	return value.(*View)
}

func (v *View) SetNamedType(aType reflect.Type) {
	v.Schema.SetType(aType)
	holderType := types.EnsureStruct(aType)
	for _, rel := range v.With {
		if holderViewType, ok := holderType.FieldByName(rel.Holder); ok {
			rel.Of.View.SetNamedType(holderViewType.Type)
		}
	}
}

func (v *View) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, ContextKey, v)
}

// Constraints configure what can be selected by Statelet
// For each _field, default value is `false`
type Constraints struct {
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

func (v *View) Resource() state.Resource {
	return NewResources(v._resource, v)
}

// OutputType returns reader view output type
func (v *View) OutputType() reflect.Type {
	if v.Schema.Cardinality == state.Many {
		return v.Schema.SliceType()
	}
	return v.Schema.Type()
}

func (v *View) Warmup() *Warmup {
	if v.Cache == nil {
		return nil
	}
	return v.Cache.Warmup
}

func (v *View) ViewName() string {
	return v.Name
}

func (v *View) InputParameters() state.Parameters {
	if v._inputParameters != nil {
		return v._inputParameters
	}
	v._inputParameters = state.Parameters{}
	v.inputParameters(&v._inputParameters)
	return v._inputParameters
}

func (v *View) inputParameters(parameters *state.Parameters) {
	if v.Template != nil {
		for i := range v.Template.Parameters {
			parameters.Append(v.Template.Parameters[i])
		}
	}
	for _, rel := range v.With {
		rel.Of.inputParameters(parameters)
	}
}

func (v *View) EnsureTemplate() {
	if v.Template == nil {
		v.Template = &Template{}
	}
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

	aResource := &Resourcelet{Resource: resource}
	for _, arg := range m.Args {
		//TODO: Check format

		if err := arg.Init(aResource); err != nil {
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
	return v.Schema.CompType()
}

func (v *View) setResource(resource *Resource) {
	if resource == nil {
		resource = EmptyResource()
	}
	v._resource = resource
	if len(v.With) == 0 {
		return
	}
	for _, rel := range v.With {
		rel.Of.View.setResource(resource)
	}
}

// Init initializes View using View provided in Resource.
// i.e. If View, Connector etc. should inherit from others - it has te bo included in Resource.
// It is important to call Init for every View because it also initializes due to the optimization reasons.
func (v *View) Init(ctx context.Context, resource *Resource, opts ...Option) error {
	if err := Options(opts).Apply(v); err != nil {
		return err
	}
	if v._initialized {
		return nil
	}
	v._initialized = true
	v.setResource(resource)
	return v.init(ctx)
}

func (v *View) init(ctx context.Context) error {
	takeNames := map[string]bool{
		v.Name: true,
	}
	lookupType := v._resource.LookupType()
	if err := v.Schema.LoadTypeIfNeeded(lookupType); err != nil {
		return err
	}
	if v.Description == "" {
		v.Description = v.Name
	}

	if err := v.ensureConnector(ctx); err != nil {
		return err
	}

	if err := v.initViewRelations(ctx, takeNames); err != nil {
		return err
	}
	if err := v.initView(ctx); err != nil {
		return err
	}

	if err := v.updateViewAndRelations(ctx, v.With); err != nil {
		return err
	}
	return nil
}

func (v *View) inheritRelationsFromTag(schema *state.Schema) error {
	sType := schema.Type()
	if sType == nil {
		sType, _ = types.LookupType(v._resource.LookupType(), schema.DataType)
		if sType == nil {
			return nil
		}
	}
	recType := ensureStruct(sType)
	if recType == nil {
		return nil
	}
	var viewOptions []Option
	for i := 0; i < recType.NumField(); i++ {
		field := recType.Field(i)
		aTag, err := tags.ParseViewTags(field.Tag, v._fs)
		if err != nil {
			return err
		}
		if len(aTag.LinkOn) == 0 {
			continue
		}

		viewTag := aTag.View
		if viewTag == nil {
			aTag.View = &tags.View{}
			viewTag = aTag.View
		}
		setter.SetStringIfEmpty(&aTag.View.Name, aTag.TypeName)
		setter.SetStringIfEmpty(&aTag.View.Name, field.Name)
		refViewOptions, err := v.buildViewOptions(field.Type, aTag)
		if err != nil {
			return err
		}
		relLinks, refLinks, err := v.buildLinks(aTag)
		if err != nil {
			return err
		}
		if viewTag.Match != "" {
			refViewOptions = append(refViewOptions, WithMatchStrategy(viewTag.Match))
		}
		if isSlice(field.Type) {
			viewOptions = append(viewOptions, WithOneToMany(field.Name, relLinks,
				NewReferenceView(refLinks, NewView(viewTag.Name, viewTag.Table, refViewOptions...))))
		} else {
			viewOptions = append(viewOptions, WithOneToOne(field.Name, relLinks,
				NewReferenceView(refLinks, NewView(viewTag.Name, viewTag.Table, refViewOptions...))))
		}
	}
	if len(viewOptions) > 0 {
		return Options(viewOptions).Apply(v)
	}

	return nil
}

func (v *View) buildViewOptions(aViewType reflect.Type, tag *tags.Tag) ([]Option, error) {
	var options []Option
	var err error
	var connector *Connector
	var parameters []*state.Parameter
	if v._fs != nil {
		options = append(options, WithFS(v._fs))
	}
	if aViewType != nil {
		options = append(options, WithViewType(aViewType))
	}
	if vTag := tag.View; vTag != nil {
		if vTag.Connector != "" {
			if connector, err = v._resource.Connector(vTag.Connector); err != nil {
				return nil, fmt.Errorf("%w, View '%v' connector: '%v'", err, vTag.Name, vTag.Connector)
			}
			options = append(options, WithConnector(connector))
		}
		if vTag.Limit != nil {
			options = append(options, WithLimit(vTag.Limit))

		}
		for _, name := range vTag.Parameters {
			parameters = append(parameters, state.NewRefParameter(name))
		}
	}
	if SQL := tag.SQL; SQL.SQL != "" {
		tmpl := NewTemplate(string(SQL.SQL), WithTemplateParameters(parameters...))
		options = append(options, WithTemplate(tmpl))
	}
	return options, nil
}

func WithLimit(limit *int) Option {
	return func(view *View) error {
		if view.Selector == nil {
			view.Selector = &Config{}
		}
		if view.Selector.Constraints == nil {
			view.Selector.Constraints = &Constraints{}
		}
		view.Selector.Constraints.Limit = true
		view.Selector.Limit = *limit
		return nil
	}
}

func (v *View) buildLinks(aTag *tags.Tag) (Links, Links, error) {
	if len(aTag.LinkOn) == 0 {
		return nil, nil, fmt.Errorf("relation not defined")
	}
	var rel, ref Links
	err := aTag.LinkOn.ForEach(func(relField, relColumn, refField, refColumn string, includeColumn *bool) error {
		relLink := &Link{Field: relField, Column: relColumn, IncludeColumn: includeColumn}
		relLink.ensureNamespace()
		rel = append(rel, relLink)
		refLink := &Link{Field: refField, Column: refColumn}
		refLink.ensureNamespace()
		ref = append(ref, refLink)
		return nil
	})
	return rel, ref, err
}

func (v *View) buildRefLinks(aTag *tags.Tag) (Links, error) {
	if len(aTag.LinkOn) == 0 {
		return nil, fmt.Errorf("relation not defined")
	}
	var result = Links{}
	err := aTag.LinkOn.ForEach(func(relField, relColumn, refField, refColumn string, includeColumn *bool) error {
		result = append(result, &Link{Field: relField, Column: relColumn, IncludeColumn: includeColumn})
		return nil
	})
	return result, err
}

func (v *View) loadFromWithURL(ctx context.Context) error {
	if v.FromURL == "" || v.From != "" {
		return nil
	}
	var err error
	v.From, err = v._resource.LoadText(ctx, v.FromURL)
	return err
}

func (v *View) initViewRelations(ctx context.Context, takenNames map[string]bool) (err error) {
	if schema := v.Schema; schema != nil && len(v.With) == 0 {
		if err = v.inheritRelationsFromTag(schema); err != nil {
			return err
		}
	}

	compType := v.ComponentType()
	relations := v.With
	for _, rel := range relations {
		refView := &rel.Of.View
		refView._parent = v
		refView._resource = v._resource
		v.generateNameIfNeeded(refView, rel)
		if err = v.updateRelationSchemaIfDefined(compType, rel); err != nil {
			return err
		}

		isNotUnique := takenNames[rel.Of.View.Name]
		if isNotUnique {
			return fmt.Errorf("not unique View name: %v", rel.Of.View.Name)
		}
		takenNames[rel.Of.View.Name] = true
		for _, transform := range v._transforms {
			pathPrefix := rel.Holder + "."
			if strings.HasPrefix(transform.Path, pathPrefix) {
				relTransform := *transform
				relTransform.Path = relTransform.Path[len(pathPrefix):]
				refView._transforms = append(refView._transforms, &relTransform)
			}
		}

		if err := refView.inheritFromViewIfNeeded(ctx); err != nil {
			return err
		}
		if refView.Connector == nil {
			refView.Connector = v.Connector
		}
		if err := rel.Init(ctx, v); err != nil {
			return err
		}
		if err := refView.initViewRelations(ctx, takenNames); err != nil {
			return err
		}
		if err := refView.initView(ctx); err != nil {
			return err
		}
		if err = rel.adjustLinkColumn(); err != nil {
			return err
		}

	}
	return nil
}

func (v *View) ComponentType() reflect.Type {
	var compType reflect.Type
	if v.Schema != nil {
		compType = v.Schema.CompType()
	}
	if compType != nil {
		compType = types.EnsureStruct(compType)
	}
	return compType
}

func (v *View) updateRelationSchemaIfDefined(compType reflect.Type, rel *Relation) (err error) {
	if compType == nil {
		return
	}
	aView := &rel.Of.View
	if aView.Schema.Type() != nil {
		return nil
	}
	field, ok := compType.FieldByName(rel.Holder)
	if !ok {
		return fmt.Errorf("invalid view '%v' relation '%v' ,failed to locate rel holder: %s, in onwer type: %s", v.Name, rel.Name, rel.Holder, compType.String())
	}
	if ref := aView.Ref; ref != "" {
		if aView, err = v._resource._views.Lookup(ref); err != nil {
			return err
		}
	}
	if aView.Schema == nil {
		aView.Schema = &state.Schema{}
	}
	aView.Schema.SetType(field.Type)
	if relView := &rel.Of.View; relView._parent != nil {
		aView._parent = relView._parent
	}
	return nil
}

func (v *View) generateNameIfNeeded(refView *View, rel *Relation) {
	if refView.Name == "" {
		refView.Name = v.Name + "#rel:" + rel.Name
	}
}

func (v *View) initView(ctx context.Context) error {
	var err error
	if err = v.loadFromWithURL(ctx); err != nil {
		return err
	}
	if err = v.inheritFromViewIfNeeded(ctx); err != nil {
		return err
	}
	if v.ColumnsConfig == nil {
		v.ColumnsConfig = map[string]*ColumnConfig{}
	}
	v.ensureIndexExcluded()
	v.ensureBatch()
	v.ensureSelector()
	if err = v.ensureLogger(); err != nil {
		return err
	}
	v.ensureCounter()

	setter.SetStringIfEmpty(&v.Alias, "t")
	if v.From == "" {
		setter.SetStringIfEmpty(&v.Table, v.Name)
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
	if v.Name == v.Ref && !v.Standalone {
		return fmt.Errorf("view name and ref cannot be the same")
	}
	if v.Name == "" {
		return fmt.Errorf("view name was empty")
	}

	if err = v.ensureConnector(ctx); err != nil {
		return err
	}

	if v.Mode == ModeQuery || v.Mode == ModeUnspecified {
		if err = v.ensureColumns(ctx, v._resource); err != nil {
			return err
		}
		v.reconcileColumnTypes()
	}

	if err = v.ensureCaseFormat(); err != nil {
		return err
	}

	if err = v.indexTransforms(); err != nil {
		return err
	}

	resourcelet := NewResources(v._resource, v)
	if err = Columns(v.Columns).ApplyConfig(v.ColumnsConfig, resourcelet.LookupType()); err != nil {
		return err
	}
	if err = Columns(v.Columns).Init(resourcelet, v.CaseFormat, v.AreNullValuesAllowed()); err != nil {
		return err
	}
	v._columns = Columns(v.Columns).Index(v.CaseFormat)
	if err = v.validateSelfRef(); err != nil {
		return err
	}
	if err = v.ensureSchema(ctx, v._resource); err != nil {
		return err
	}
	if err = v.Selector.Init(ctx, v._resource, v); err != nil {
		return err
	}
	if err = v.markColumnsAsFilterable(); err != nil {
		return err
	}

	v.updateColumnTypes()

	if err = v.initTemplate(ctx, v._resource); err != nil {
		return err
	}

	if v.Cache != nil {
		if err = v.Cache.init(ctx, v._resource, v); err != nil {
			return err
		}
	}

	v._codec, err = newColumnsCodec(v.Schema.CompType(), v.Columns)
	if err != nil {
		return err
	}

	if v.TableBatches == nil {
		v.TableBatches = map[string]bool{}
	}
	return nil
}

func (v *View) ensureSelector() {
	if v.Selector == nil {
		v.Selector = &Config{}
	}
}

func (v *View) reconcileColumnTypes() {
	if rType := v.Schema.Type(); rType != nil {
		aStruct := types.EnsureStruct(rType)
		index := map[string]*reflect.StructField{}
		for i := 0; i < aStruct.NumField(); i++ {
			field := aStruct.Field(i)
			index[field.Name] = &field
			if tag := io.ParseTag(field.Tag); tag != nil {
				index[tag.Column] = &field
			}
		}

		for i := range v.Columns {
			col := v.Columns[i]
			if col.Codec != nil {
				if colType, err := types.LookupType(v._resource.LookupType(), col.DataType); err == nil {
					col.SetColumnType(colType)
					continue
				}
			}
			if field, ok := index[col.Name]; ok {
				if col.rType != field.Type {
					fieldType := field.Type
					if fieldType.Kind() == reflect.Ptr {
						fieldType = fieldType.Elem()
					}
					col.rType = fieldType
					if name := col.rType.Name(); name != "" {
						col.DataType = name
					}
				}
			}
		}
	}
}

func (v *View) GetSchema(ctx context.Context) (*state.Schema, error) {
	if v.Schema != nil {
		if v.Schema.Type() != nil {
			return v.Schema, nil
		}
		if v.Schema.DataType != "" {
			err := v.Schema.InitType(v._resource.LookupType(), false)
			return v.Schema, err
		}
	}
	if err := v.init(ctx); err != nil {
		return nil, err
	}
	return v.Schema, nil
}

func (v *View) ensureConnector(ctx context.Context) error {
	if v.Connector != nil && v.Connector._initialized {
		return nil
	}

	var err error
	if v.Connector, err = v._resource.FindConnector(v); err != nil {
		return err
	}

	if err = v.Connector.Init(ctx, v._resource._connectors); err != nil {
		return err
	}

	if err = v.Connector.Validate(); err != nil {
		return err
	}
	return nil
}

func (v *View) ensureCounter() {
	if v.Counter != nil {
		return
	}
	var counter logger.Counter
	if metric := v._resource.Metrics; metric != nil {
		name := v.Name
		if metric.Method != "" && metric.Method != http.MethodGet {
			name = metric.Method + ":" + name
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

func (c *View) TypeDefinitions() []*TypeDefinition {
	if c._resource == nil {
		return nil
	}
	return c._resource.Types
}

func (v *View) updateColumnTypes() {
	if len(v._columns) == 0 {
		return
	}
	rType := shared.Elem(v.DataType())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)

		column, err := v._columns.Lookup(field.Name)
		if err != nil {
			continue
		}

		column.SetField(field)
	}
}

func (v *View) updateViewAndRelations(ctx context.Context, relations []*Relation) error {
	v.indexColumns()
	if err := v.indexSqlxColumnsByFieldName(); err != nil {
		return err
	}
	v.ensureCollector()
	for _, rel := range relations {
		if err := rel.Init(ctx, v); err != nil {
			return err
		}
		refView := rel.Of.View
		if err := refView.updateViewAndRelations(ctx, refView.With); err != nil {
			return err
		}
	}
	if err := v.registerHolders(); err != nil {
		return err
	}
	return nil
}

func (v *View) ensureColumns(ctx context.Context, resource *Resource) error {
	if resource.viewColumns != nil {
		if cachedColumns, ok := resource.viewColumns[v.Name]; ok {
			v.Columns = cachedColumns
		}
	}
	if len(v.Columns) != 0 {
		return nil
	}
	//if scheme type defines sqlx tag, use it as source for column instead of detection
	if rType := v.Schema.Type(); rType != nil {
		sType := types.EnsureStruct(rType)
		if columns := io.ExtractColumnNames(sType); len(columns) > 0 {
			if columns, err := io.StructColumns(sType, "sqlx"); err == nil {
				v.Columns = convertIoColumnsToColumns(columns, make(map[string]bool))
			}
		}
	}

	if len(v.Columns) != 0 {
		return nil
	}
	if v.Mode == "Write" || v.Mode == ModeExec || v.Mode == ModeHandler {
		return nil
	}

	err := v.detectColumns(ctx, resource)
	if err != nil {
		return err
	}
	if resource.viewColumns != nil {
		resource.viewColumns[v.Name] = v.Columns
	}
	return nil
}

func (v *View) detectColumns(ctx context.Context, resource *Resource) error {
	SQL := v.Source()
	var aState state.Parameters
	if v.Template != nil {
		if err := v.Template.Init(ctx, resource, v); err != nil {
			return err
		}
		SQL = v.Template.Source
		aState = v.Template.Parameters
	}
	var options []expand2.StateOption
	var bindingArguments []interface{}

	if strings.Contains(SQL, "$View.ParentJoinOn") {
		//TODO adjust parameter value type
		options = append(options, expand2.WithViewParam(&expand2.MetaParam{ParentValues: []interface{}{0}, DataUnit: &expand2.DataUnit{}}))
	}
	query, err := v.BuildParametrizedSQL(aState, resource.TypeRegistry(), SQL, bindingArguments, options...)
	v.Logger.ColumnsDetection(query.Query, v.Source())
	if err != nil {
		return fmt.Errorf("failed to build parameterized query: %v due to %w", SQL, err)
	}
	db, err := v.Connector.DB()
	if err != nil {
		return err
	}
	sqlColumns, err := column.Discover(ctx, db, v.Table, query.Query, query.Args...)
	if err != nil {
		return fmt.Errorf("failed to detect column with: %v due to %w", query.Query, err)
	}
	v.Columns = NewColumns(sqlColumns)
	return nil
}

func convertIoColumnsToColumns(ioColumns []io.Column, nullable map[string]bool) []*Column {
	columns := make([]*Column, 0)
	for i := 0; i < len(ioColumns); i++ {
		scanType := ioColumns[i].ScanType()
		dataTypeName := ioColumns[i].DatabaseTypeName()
		isNullable, _ := ioColumns[i].Nullable()
		columnTag := ""
		if tag := ioColumns[i].Tag(); tag != nil {
			columnTag = tag.Raw
		}
		aColum := NewColumn(ioColumns[i].Name(), dataTypeName, scanType, nullable[ioColumns[i].Name()] || isNullable, WithColumnTag(columnTag))
		aTag, _ := tags.ParseStateTags(reflect.StructTag(aColum.Tag), nil)
		if aTag != nil {
			if aTag.Format != nil {
				aColum.FormatTag = aTag.Format
			}
			if codec := aTag.Codec; codec != nil {
				aColum.Codec = &state.Codec{Name: codec.Name, Args: codec.Arguments}
			}

		}
		columns = append(columns, aColum)
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

// Source returns database View source. It prioritizes From, Table then View.Name
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

func (v *View) ensureSchema(ctx context.Context, resource *Resource) (err error) {
	v.initSchemaIfNeeded()
	if err = v.Schema.LoadTypeIfNeeded(resource.TypeRegistry().Lookup); err != nil || v.Schema.Type() != nil {
		return err
	}
	v.Schema = state.NewSchema(nil,
		state.WithMany(),
		state.WithAutoGenFunc(v.generateSchemaTypeFromColumn(v.CaseFormat, v.Columns, v.With)))
	aResource := &Resourcelet{Resource: resource}
	return v.Schema.Init(aResource)
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

	setter.SetStringIfEmpty(&v.Alias, view.Alias)
	setter.SetStringIfEmpty(&v.Table, view.Table)
	setter.SetStringIfEmpty(&v.Module, view.Module)

	setter.SetStringIfEmpty(&v.Description, view.Description)

	setter.SetStringIfEmpty(&v.From, view.From)
	setter.SetStringIfEmpty(&v.FromURL, view.FromURL)
	v.Mode = Mode(shared.FirstNotEmpty(string(v.Mode), string(view.Mode)))

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
	if v.Counter == nil {
		v.Counter = view.Counter
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
		v.CaseFormat = text.DetectCaseFormat(columnNames...)
	}
	if v.CaseFormat != "" && !v.CaseFormat.IsDefined() {
		return fmt.Errorf("unsupported case format: %v", v.CaseFormat)
	}
	return nil
}

func (v *View) ensureCollector() {
	v._newCollector = func(dest interface{}, viewMetaHandler viewMetaHandlerFn, readAll bool) *Collector {
		return NewCollector(v.Schema.Slice(), v, dest, viewMetaHandler, readAll)
	}

}

// Collector creates new Collector for View.DataType
func (v *View) Collector(dest interface{}, handleMeta viewMetaHandlerFn, supportParallel bool) *Collector {
	return v._newCollector(dest, handleMeta, supportParallel)
}

func (v *View) registerHolders() error {
	for i := range v.With {

		if err := v._columns.RegisterHolder(v.With[i].On[0].Column, v.With[i].Holder); err != nil {
			return err
		}
	}

	return nil
}

// CanUseSelectorCriteria indicates if Template.Criteria can be used
func (v *View) CanUseSelectorCriteria() bool {
	return v.Selector.Constraints.Criteria
}

// CanUseSelectorLimit indicates if Template.Limit can be used
func (v *View) CanUseSelectorLimit() bool {
	return v.Selector.Constraints.Limit
}

// CanUseSelectorOrderBy indicates if Template.OrderBy can be used
func (v *View) CanUseSelectorOrderBy() bool {
	return v.Selector.Constraints.OrderBy
}

// CanUseSelectorOffset indicates if Template.Offset can be used
func (v *View) CanUseSelectorOffset() bool {
	return v.Selector.Constraints.Offset
}

// CanUseSelectorProjection indicates if Template.Fields can be used
func (v *View) CanUseSelectorProjection() bool {
	return v.Selector.Constraints.Projection
}

// IndexedColumns returns Columns
func (v *View) IndexedColumns() NamedColumns {
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
			return fmt.Errorf("criteria column %v, on view has not been defined, %w", colName, v.Name, err)
		}
		column.Filterable = true
	}
	return nil
}

func (v *View) indexSqlxColumnsByFieldName() error {
	if len(v._columns) == 0 {
		return nil
	}
	rType := shared.Elem(v.Schema.Type())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		isExported := field.PkgPath == ""
		if !isExported {
			continue
		}

		tag := io.ParseTag(field.Tag)
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

func (v *View) updateColumn(ns string, rType reflect.Type, columns *[]*Column, relation *Relation, columnsIndex NamedColumns) error {
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

		//TODO analyze usage of io.ParseTag to simplify
		sqlxTag := io.ParseTag(field.Tag)
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
			columnsIndex.Register(v.CaseFormat, column)
		}
	}

	for _, rel := range v.With {
		for i, item := range rel.Of.On {
			parent := rel.On[i]
			if _, ok := columnsIndex[item.Column]; ok {
				continue
			}
			col, err := v._columns.Lookup(parent.Column)
			if err != nil {
				return fmt.Errorf("invalid rel: %v %w", rel.Name, err)
			}
			*columns = append(*columns, col)
		}
	}

	if relation != nil {
		for _, item := range relation.Of.On {
			_, err := columnsIndex.Lookup(item.Column)
			if err != nil {
				col, err := v._columns.Lookup(item.Column)
				if err != nil {
					return fmt.Errorf("invalid ref: %v %w", relation.Name, err)
				}
				*columns = append(*columns, col)
			}
		}
	}

	return nil
}

func (v *View) initSchemaIfNeeded() {
	if v.Schema == nil {
		v.Schema = state.NewSchema(nil, state.WithMany(), state.WithAutoGenFlag(true))
	}
}

func (v *View) inheritFromViewIfNeeded(ctx context.Context) error {
	if v.Ref != "" {
		view, err := v._resource._views.Lookup(v.Ref)
		if err != nil {
			return err
		}

		if err = view.Init(ctx, v._resource); err != nil {
			return err
		}

		if err = v.inherit(view); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) indexColumns() {
	v._columns = Columns(v.Columns).Index(v.CaseFormat)
}

func (v *View) ensureLogger() error {
	if v.Logger == nil {
		v.Logger = logger.Default()
		return nil
	}

	if v.Logger.Ref != "" {
		adapter, ok := v._resource._loggers.Lookup(v.Logger.Ref)
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
	v.EnsureTemplate()
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

	return v.Schema.CompType()
}

func (v *View) UnwrapDatabaseType(ctx context.Context, value interface{}) (interface{}, error) {
	if v._codec != nil {
		actualRecord := v._codec.unwrapper.Value(xunsafe.AsPointer(value))
		if err := v._codec.updateValue(ctx, value, &codec.ParentValue{Value: actualRecord, RType: v.Schema.CompType()}); err != nil {
			return nil, err
		}
		actualRecord = v._codec.unwrapper.Value(xunsafe.AsPointer(value))
		return actualRecord, nil
	}
	return value, nil
}

func (v *View) indexTransforms() error {
	if len(v._transforms) == 0 {
		return nil
	}
	for _, transform := range v._transforms {
		if strings.Contains(transform.Path, ".") {
			continue
		}

		columnName := text.CaseFormatUpperCamel.Format(transform.Path, v.CaseFormat)
		aConfig, ok := v.ColumnsConfig[columnName]
		if !ok {
			aConfig = &ColumnConfig{}
			v.ColumnsConfig[columnName] = aConfig
		}

		aCodec, ok := v._resource.CodecByName(transform.Codec)
		if !ok {
			return fmt.Errorf("not found codec %v", transform.Codec)
		}

		codecInstance := aCodec.Instance
		resultType, err := codecInstance.ResultType(nil)
		if err != nil {
			return err
		}
		aConfig.Codec = state.NewCodec(transform.Codec, state.NewSchema(resultType), codecInstance)
	}

	return nil
}

func (v *View) Expand(placeholders *[]interface{}, SQL string, selector *Statelet, params CriteriaParam, batchData *BatchData, sanitized *expand2.DataUnit) (string, error) {
	v.ensureParameters(selector)

	return v.Template.Expand(placeholders, SQL, selector, params, batchData, sanitized)
}

func (v *View) ensureParameters(selector *Statelet) {
	if v.Template == nil {
		return
	}
	selector.Init(v)
}

func (v *View) ParamByName(name string) (*state.Parameter, error) {
	return v.Template._parametersIndex.Lookup(name)
}

func (v *View) MetaTemplateEnabled() bool {
	return v.Template.Summary != nil
}

func (v *View) AreNullValuesAllowed() bool {
	return v.AllowNulls != nil && *v.AllowNulls
}

func (v *View) validateSelfRef() error {
	if v.SelfReference == nil {
		return nil
	}

	if v.SelfReference.Holder == "" {
		return fmt.Errorf("View %v SelfReference Holder can't be empty", v.Name)
	}

	if v.SelfReference.Child == "" {
		return fmt.Errorf("View %v SelfReference Child can't be empty", v.Name)
	}

	if _, err := v._columns.Lookup(v.SelfReference.Child); err != nil {
		return err
	}

	if v.SelfReference.Parent == "" {
		return fmt.Errorf("View %v SelfReference locators can't be empty", v.Name)
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

// SetParameter sets a View or relation parameter, for relation name has to be prefixed relName:paramName
func (v *View) SetParameter(name string, selectors *State, value interface{}) error {
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
	return param.Set(selector.Template, value)
}

func (v *View) BuildParametrizedSQL(aState state.Parameters, types *xreflect.Types, SQL string, bindingArgs []interface{}, options ...expand2.StateOption) (*sqlx.SQL, error) {
	reflectType, err := aState.ReflectType(pkgPath, types.Lookup, state.WithSetMarker())
	if err != nil {
		return nil, fmt.Errorf("failed to create aState %v type: %w", v.Name, err)
	}
	stateType := structology.NewStateType(reflectType)
	inputState := stateType.NewState()

	if err = aState.SetLiterals(inputState); err != nil {
		return nil, err
	}
	if err := aState.InitRepeated(inputState); err != nil {
		return nil, err
	}
	options = append(options, expand2.WithParameterState(inputState))

	evaluator, err := NewEvaluator(aState, stateType, SQL, types.Lookup, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create evaluator %v: %w", v.Name, err)
	}
	result, err := evaluator.Evaluate(nil, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate %v: %w", v.Name, err)
	}
	bindingArgs = append(bindingArgs, result.Context.DataUnit.ParamsGroup...)
	result.Expanded = shared.TrimPair(result.Expanded, '(', ')')
	return &sqlx.SQL{Query: result.Expanded, Args: bindingArgs}, nil
}

func (v *View) SetResource(resource *Resource) {
	v._resource = resource
}

func (v *View) GetResource() *Resource {
	return v._resource
}

func NewRefView(ref string) *View {
	return &View{Reference: shared.Reference{Ref: ref}}
}

// NewView creates a View
func NewView(name, table string, opts ...Option) *View {
	ret, err := New(name, table, opts...)
	if err != nil {
		panic(err.Error())
	}
	return ret
}

// New creates a View
func New(name, table string, opts ...Option) (*View, error) {
	ret := &View{Name: name, Table: table}
	if err := Options(opts).Apply(ret); err != nil {
		return nil, fmt.Errorf("failed to create view %s,  %v", ret.Name, err)
	}

	return ret, nil
}

// NewExecView creates an execution View
func NewExecView(name, table string, template string, parameters []*state.Parameter, opts ...Option) *View {
	var templateParameters []TemplateOption
	for i := range parameters {
		templateOption := WithTemplateParameters(parameters[i])
		templateParameters = append(templateParameters, templateOption)
	}
	opts = append(opts, WithViewKind(ModeExec),
		WithTemplate(NewTemplate(template, templateParameters...)))
	return NewView(name, table, opts...)
}

// WithMatchStrategy creates an Option to set MatchStrategy
func WithMatchStrategy(match string) Option {
	return func(v *View) error {
		v.MatchStrategy = MatchStrategy(match)
		return nil
	}
}
