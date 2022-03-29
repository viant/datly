package data

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data/ast"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
)

type (
	//View represents a data View
	View struct {
		shared.Reference
		Connector *config.Connector
		Name      string
		Alias     string `json:",omitempty"`
		Table     string `json:",omitempty"`
		From      string `json:",omitempty"`

		Exclude              []string   `json:",omitempty"`
		Columns              []*Column  `json:",omitempty"`
		InheritSchemaColumns bool       `json:",omitempty"`
		CaseFormat           CaseFormat `json:",omitempty"`

		Criteria *Criteria `json:",omitempty"`

		Selector            *Config      `json:",omitempty"`
		SelectorConstraints *Constraints `json:",omitempty"`
		Parameters          []*Parameter `json:",omitempty"`

		Prefix string  `json:",omitempty"`
		Schema *Schema `json:",omitempty"`

		With []*Relation `json:",omitempty"`

		MatchStrategy MatchStrategy `json:",omitempty"`
		BatchReadSize *int          `json:",omitempty"`

		_columns    Columns
		_excluded   map[string]bool
		_parameters Parameters
		_paramField *xunsafe.Field

		//For optimization reasons. All of those Parameters and _allRequiredParameters contains also relation parameters
		//same goes to the views.
		//If View has 4 Relations, lookup would take 4 times longer.
		_cookiesKind           Parameters
		_headerKind            Parameters
		_pathKind              Parameters
		_queryKind             Parameters
		_allRequiredParameters []*Parameter
		_views                 *Views

		Caser              format.Case `json:",omitempty"`
		initialized        bool
		holdersInitialized bool
		isValid            bool
		newCollector       func(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector

		hasCriteriaReplacement bool
		hasColumnInReplacement bool
		hasWhereClause         bool
		hasPagination          bool
	}

	//Constraints configure what can be selected by Selector
	//For each field, default value is `false`
	Constraints struct {
		Criteria          bool
		OrderBy           bool
		Limit             bool
		Columns           bool
		Offset            bool
		FilterableColumns []string
		Alias             bool
	}
)

//DataType returns struct type.
func (v *View) DataType() reflect.Type {
	return v.Schema.Type()
}

//Init initializes View using data provided in Resource.
//i.e. If View, Connector etc. should inherit from others - it has te bo included in Resource.
//It is important to call Init for every View because it also initializes due to the optimization reasons.
func (v *View) Init(ctx context.Context, resource *Resource) error {
	if v.initialized {
		return nil
	}

	if err := v.initViews(ctx, resource, v.With); err != nil {
		return err
	}

	if err := v.initView(ctx, resource); err != nil {
		return err
	}

	if err := v.initAfterViewsInitialized(ctx, resource, v.With); err != nil {
		return err
	}
	v.initialized = true

	return nil
}

func (v *View) initViews(ctx context.Context, resource *Resource, relations []*Relation) error {
	for _, rel := range relations {
		refView := &rel.Of.View
		v.generateNameIfNeeded(refView, rel)
		if err := refView.inheritFromViewIfNeeded(ctx, resource); err != nil {
			return err
		}

		if err := refView.initViews(ctx, resource, refView.With); err != nil {
			return err
		}

		if err := refView.initView(ctx, resource); err != nil {
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

func (v *View) initView(ctx context.Context, resource *Resource) error {
	var err error
	v.ensureViewIndexed()
	if err = v.inheritFromViewIfNeeded(ctx, resource); err != nil {
		return err
	}

	if v.MatchStrategy == "" {
		v.MatchStrategy = ReadMatched
	}
	if err = v.MatchStrategy.Validate(); err != nil {
		return err
	}

	v.Alias = notEmptyOf(v.Alias, "t")
	v.Table = notEmptyOf(v.Table, v.Name)

	if v.Selector == nil {
		v.Selector = &Config{}
	}

	if v.SelectorConstraints == nil {
		v.SelectorConstraints = &Constraints{}
	}

	if v.Name == v.Ref {
		return fmt.Errorf("view name and ref cannot be the same")
	}

	if v.Name == "" {
		return fmt.Errorf("view name was empty")
	}

	if v.Connector, err = resource.FindConnector(v); err != nil {
		return err
	}

	if err = v.Connector.Init(ctx, resource._connectors); err != nil {
		return err
	}

	if err = v.Connector.Validate(); err != nil {
		return err
	}

	if err = v.ensureCaseFormat(); err != nil {
		return err
	}

	if err = v.ensureColumns(ctx); err != nil {
		return err
	}

	if err = ColumnSlice(v.Columns).Init(); err != nil {
		return err
	}
	v._columns = ColumnSlice(v.Columns).Index(v.Caser)
	if err = v.markColumnsAsFilterable(); err != nil {
		return err
	}

	v.ensureIndexExcluded()
	v.ensureSelectorConstraints()

	if err = v.ensureSchema(resource.types); err != nil {
		return err
	}
	v.initColumnsPositions()
	v.updateColumnTypes()
	return nil
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

func (v *View) initAfterViewsInitialized(ctx context.Context, resource *Resource, relations []*Relation) error {
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
		if err := refView.initAfterViewsInitialized(ctx, resource, refView.With); err != nil {
			return err
		}
	}

	if err := v.collectFromRelations(ctx, resource); err != nil {
		return err
	}

	if err := v.registerHolders(); err != nil {
		return err
	}

	return nil
}

func (v *View) ensureColumns(ctx context.Context) error {
	if len(v.Columns) != 0 {
		return nil
	}

	db, err := v.Connector.Db()
	if err != nil {
		return err
	}

	source := v.columnsSource()
	SQL := "SELECT t.* FROM " + source + " t WHERE 1=0"
	shared.Log("table columns SQL: %v", SQL)
	query, err := db.QueryContext(ctx, SQL)
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

func (v *View) columnsSource() string {
	source := v.Source()
	if strings.Contains(source, string(shared.Criteria)) {
		if v.hasWhereClause {
			source = strings.ReplaceAll(source, string(shared.Criteria), " AND 1 = 0")
		} else {
			source = strings.ReplaceAll(source, string(shared.Criteria), " WHERE 1 = 0")
		}
	}

	if strings.Contains(source, string(shared.ColumnInPosition)) {
		source = strings.ReplaceAll(source, string(shared.ColumnInPosition), " 1 = 0")
	}

	if strings.Contains(source, string(shared.Pagination)) {
		source = strings.ReplaceAll(source, string(shared.Pagination), " ")
	}

	return source
}

func convertIoColumnsToColumns(ioColumns []io.Column) []*Column {
	columns := make([]*Column, 0)
	for i := 0; i < len(ioColumns); i++ {
		scanType := ioColumns[i].ScanType()
		dataTypeName := ioColumns[i].DatabaseTypeName()
		columns = append(columns, &Column{
			Name:     ioColumns[i].Name(),
			DataType: dataTypeName,
			rType:    scanType,
		})
	}
	return columns
}

//ColumnByName returns Column by Column.Name
func (v *View) ColumnByName(name string) (*Column, bool) {
	if column, ok := v._columns[name]; ok {
		return column, true
	}

	return nil, false
}

//Source returns database data source. It prioritizes From, Table then View.Name
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
		componentType := types.Lookup(v.Schema.Name)
		if componentType == nil {
			return fmt.Errorf("not found type for Schema %v", v.Schema.Name)
		}

		if componentType != nil {
			v.Schema.setType(componentType)
		}
	}

	v.Schema.Init(v.Columns, v.With, v.Caser)

	return nil
}

//Db returns database connection that View was assigned to.
func (v *View) Db() (*sql.DB, error) {
	return v.Connector.Db()
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

	if v.Schema == nil && len(v.With) == 0 {
		v.Schema = view.Schema
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

	if v.SelectorConstraints == nil {
		v.SelectorConstraints = view.SelectorConstraints
	}
}

func (v *View) ensureIndexExcluded() {
	if len(v.Exclude) == 0 {
		return
	}

	v._excluded = Names(v.Exclude).Index()
}

//SelectedColumns returns columns selected by Selector if it is allowed by the View to use Selector.Columns
//(see Constraints.Columns) or View.Columns
func (v *View) SelectedColumns(selector *Selector) ([]*Column, error) {
	if !v.CanUseSelectorColumns() || selector == nil || len(selector.Columns) == 0 {
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
	if err := v.CaseFormat.Init(); err != nil {
		return err
	}

	var err error
	v.Caser, err = v.CaseFormat.Caser()
	return err
}

func (v *View) ensureCollector() {
	v.newCollector = func(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector {
		return NewCollector(v.Schema.slice, v, dest, supportParallel)
	}
}

//Collector creates new Collector for View.DataType
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

func (v *View) registerHolders() error {
	for i := range v.With {
		if err := v._columns.RegisterHolder(v.With[i]); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) collectFromRelations(ctx context.Context, resource *Resource) error {
	for _, param := range v.Parameters {
		if err := param.Init(ctx, resource); err != nil {
			return err
		}
	}

	v._parameters = ParametersSlice(v.Parameters).Index()
	v._cookiesKind = ParametersSlice(v.Parameters).Filter(CookieKind)
	v._headerKind = ParametersSlice(v.Parameters).Filter(HeaderKind)
	v._pathKind = ParametersSlice(v.Parameters).Filter(PathKind)
	v._queryKind = ParametersSlice(v.Parameters).Filter(QueryKind)
	v._allRequiredParameters = v.FilterRequiredParams()

	v.appendReferencesParameters()
	return nil
}

//LimitWithSelector returns Selector.Limit if it is allowed by the View to use Selector.Columns (see Constraints.Limit)
func (v *View) LimitWithSelector(selector *Selector) int {
	if v.CanUseSelectorLimit() && selector != nil && selector.Limit > 0 {
		return selector.Limit
	}
	return v.Selector.Limit
}

//UsesCookie returns true if View or any of relations View Parameter uses cookie.
func (v *View) UsesCookie(cookie *http.Cookie) bool {
	param, _ := v._cookiesKind.Lookup(cookie.Name)
	if param != nil {
		return true
	}

	return false
}

//UsesUriParam returns true if View or any of relations View Parameter uses path variable param.
func (v *View) UsesUriParam(paramName string) bool {
	param, _ := v._pathKind.Lookup(paramName)
	if param != nil {
		return true
	}

	return false
}

//UsesHeader returns true if View or any of relations View Parameter uses header.
func (v *View) UsesHeader(headerName string) bool {
	param, _ := v._headerKind.Lookup(headerName)
	if param != nil {
		return true
	}
	return false
}

//UsesQueryParam returns true if View or any of relations View Parameter uses query param.
func (v *View) UsesQueryParam(paramName string) bool {
	param, _ := v._queryKind.Lookup(paramName)
	if param != nil {
		return true
	}
	return false
}

func (v *View) ensureSelectorConstraints() {
	if v.SelectorConstraints == nil {
		v.SelectorConstraints = &Constraints{}
	}

}

//CanUseSelectorCriteria indicates if Selector.Criteria can be used
func (v *View) CanUseSelectorCriteria() bool {
	return v.SelectorConstraints.Criteria
}

//CanUseSelectorColumns indicates if Selector.Columns can be used
func (v *View) CanUseSelectorColumns() bool {
	return v.SelectorConstraints.Columns
}

//CanUseSelectorAlias indicates if Selector.Alias can be used
func (v *View) CanUseSelectorAlias() bool {
	return v.SelectorConstraints.Alias
}

//CanUseSelectorLimit indicates if Selector.Limit can be used
func (v *View) CanUseSelectorLimit() bool {
	return v.SelectorConstraints.Limit
}

//CanUseSelectorOrderBy indicates if Selector.OrderBy can be used
func (v *View) CanUseSelectorOrderBy() bool {
	return v.SelectorConstraints.OrderBy
}

//CanUseSelectorOffset indicates if Selector.Offset can be used
func (v *View) CanUseSelectorOffset() bool {
	return v.SelectorConstraints.Offset
}

//FilterRequiredParams returns all required parameters, including relations View
func (v *View) FilterRequiredParams() []*Parameter {
	if v._allRequiredParameters != nil {
		return v._allRequiredParameters
	}

	result := make([]*Parameter, 0)
	for i := range v.Parameters {
		if v.Parameters[i].IsRequired() {
			result = append(result, v.Parameters[i])
		}
	}

	for i := range v.With {
		result = append(result, (&v.With[i].Of.View).FilterRequiredParams()...)
	}

	return result
}

func (v *View) appendReferencesParameters() {
	for _, rel := range v.With {
		relationView := &rel.Of.View
		relationView.appendReferencesParameters()
		relationView.ensureViewIndexed()
		v.mergeParams(relationView)
	}
}

func (v *View) mergeParams(view *View) {
	v._cookiesKind.merge(view._cookiesKind)
	v._pathKind.merge(view._pathKind)
	v._headerKind.merge(view._headerKind)
	v._queryKind.merge(view._queryKind)
	v._views.merge(view._views)
}

//AnyOfViews returns View or any of his relation View by View.Name
func (v *View) AnyOfViews(name string) (*View, error) {
	return v._views.Lookup(name)
}

//IndexedColumns returns Columns
func (v *View) IndexedColumns() Columns {
	return v._columns
}

func (v *View) ensureViewIndexed() {
	if v._views != nil {
		return
	}

	v._views = &Views{}
	v._views.Register(v)
}

func (v *View) markColumnsAsFilterable() error {
	for _, colName := range v.SelectorConstraints.FilterableColumns {
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
		if !field.IsExported() {
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
	v._columns = ColumnSlice(newColumns).Index(v.Caser)

	return nil
}

func (v *View) updateColumn(rType reflect.Type, columns *[]*Column, relation *Relation) error {
	index := ColumnSlice(*columns).Index(v.Caser)

	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() {
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

func (v *View) ParamField() *xunsafe.Field {
	return v._paramField
}

func (v *View) initSchemaIfNeeded() {
	if v.Schema == nil {
		v.Schema = &Schema{
			autoGen: true,
		}
	}
}

func (v *View) inheritFromViewIfNeeded(ctx context.Context, resource *Resource) error {
	if v.Ref != "" {
		view, err := resource._views.Lookup(v.Ref)
		if err != nil {
			return err
		}

		if err = view.initView(ctx, resource); err != nil {
			return err
		}
		v.inherit(view)
	}
	return nil
}

func (v *View) indexColumns() {
	v._columns = ColumnSlice(v.Columns).Index(v.Caser)
}

func (v *View) AliasWith(selector *Selector) string {
	if !v.SelectorConstraints.Alias || selector == nil || selector.Alias == "" {
		return v.Alias
	}

	return selector.Alias
}

func (v *View) HasCriteriaReplacement() bool {
	return v.hasCriteriaReplacement
}

func (v *View) HasColumnInReplacement() bool {
	return v.hasColumnInReplacement
}

func (v *View) initColumnsPositions() {
	v.hasCriteriaReplacement = strings.Contains(v.Source(), string(shared.Criteria))
	v.hasColumnInReplacement = strings.Contains(v.Source(), string(shared.ColumnInPosition))
	v.hasWhereClause = ast.HasWhere([]byte(v.Source()))
	v.hasPagination = strings.Contains(v.Source(), string(shared.Pagination))
}

func (v *View) HasWhereClause() bool {
	return v.hasWhereClause
}

func (v *View) HasPaginationReplacement() bool {
	return v.hasPagination
}

//ViewReference creates a view reference
func ViewReference(name, ref string) *View {
	return &View{
		Name:      name,
		Reference: shared.Reference{Ref: ref},
	}
}
