package data

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
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

		Exclude    []string  `json:",omitempty"`
		Columns    []*Column `json:",omitempty"`
		CaseFormat string    `json:",omitempty"`

		Criteria *Criteria `json:",omitempty"`

		Selector            *Config      `json:",omitempty"`
		SelectorConstraints *Constraints `json:",omitempty"`
		Parameters          []*Parameter `json:",omitempty"`

		Prefix string  `json:",omitempty"`
		Schema *Schema `json:",omitempty"`

		With       []*Relation `json:",omitempty"`
		ParamField *xunsafe.Field `json:"_,omitempty"`

		MatchStrategy MatchStrategy `json:",omitempty"`
		BatchReadSize *int          `json:",omitempty"`

		_columns    Columns
		_excluded   map[string]bool
		_parameters Parameters

		_cookiesKind           Parameters
		_headerKind            Parameters
		_pathKind              Parameters
		_queryKind             Parameters
		_allRequiredParameters []*Parameter

		Caser        format.Case `json:",omitempty"`
		initialized  bool
		isValid      bool
		newCollector func(allowUnmapped bool, dest interface{}, supportParallel bool) *Collector
	}

	//Constraints configure what can be selected by Selector
	//For each field, default value is `false`
	Constraints struct {
		Criteria  *bool
		_criteria bool
		OrderBy   *bool
		_orderBy  bool
		Limit     *bool
		_limit    bool
		Columns   *bool
		_columns  bool
		Offset    *bool
		_offset   bool
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
	if err := v.ensureCaseFormat(); err != nil {
		return err
	}

	if err = v.ensureColumns(ctx); err != nil {
		return err
	}
	v._columns = ColumnSlice(v.Columns).Index(v.Caser)
	if err = v.initRelations(ctx, resource); err != nil {
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
	v.ensureSelectorConstraints()
	if err = v.registerHolders(); err != nil {
		return err
	}
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
		return ColumnSlice(v.Columns).Init()
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
	if !v.CanUseClientColumns() || selector == nil || len(selector.Columns) == 0 {
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

		relation.hasColumnField = relation.columnField != nil
		if relation.Cardinality == "Many" && !relation.hasColumnField {
			return fmt.Errorf("column %v doesn't have corresponding field in the struct: %v", columnName, v.DataType().String())
		}
	}

	return nil
}

func (v *View) registerHolders() error {
	for i := range v.With {
		if err := v._columns.RegisterHolder(v.With[i]); err != nil {
			return err
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

	v._parameters = ParametersSlice(v.Parameters).Index()
	v._cookiesKind = ParametersSlice(v.Parameters).Filter(CookieKind)
	v._headerKind = ParametersSlice(v.Parameters).Filter(HeaderKind)
	v._pathKind = ParametersSlice(v.Parameters).Filter(PathKind)
	v._queryKind = ParametersSlice(v.Parameters).Filter(QueryKind)

	v._allRequiredParameters = v.filterRequiredParams()

	v.appendReferencesParameters()
	return nil
}

//LimitWithSelector returns Selector.Limit if it is allowed by the View to use Selector.Columns (see Constraints.Limit)
func (v *View) LimitWithSelector(selector *Selector) int {
	if v.CanUseClientLimit() && selector != nil && selector.Limit > 0 {
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

func (v *View) shouldIndexCookie(cookie *http.Cookie) bool {
	param, _ := v._cookiesKind.Lookup(cookie.Name)
	if param != nil {
		return true
	}

	return false
}

func (v *View) shouldIndexUriParam(key string) bool {
	param, _ := v._pathKind.Lookup(key)
	if param != nil {
		return true
	}

	return false
}

func (v *View) shouldIndexHeader(key string) bool {
	param, _ := v._headerKind.Lookup(key)
	if param != nil {
		return true
	}
	return false
}

func (v *View) shouldIndexQueryParam(key string) bool {
	param, _ := v._queryKind.Lookup(key)
	if param != nil {
		return true
	}
	return false
}

func (v *View) ensureSelectorConstraints() {
	if v.SelectorConstraints == nil {
		v.SelectorConstraints = &Constraints{}
	}

	v.SelectorConstraints._criteria = v.SelectorConstraints.Criteria != nil && *v.SelectorConstraints.Criteria == true
	v.SelectorConstraints._columns = v.SelectorConstraints.Columns != nil && *v.SelectorConstraints.Columns == true
	v.SelectorConstraints._orderBy = v.SelectorConstraints.OrderBy != nil && *v.SelectorConstraints.OrderBy == true
	v.SelectorConstraints._limit = v.SelectorConstraints.Limit != nil && *v.SelectorConstraints.Limit == true
	v.SelectorConstraints._offset = v.SelectorConstraints.Offset != nil && *v.SelectorConstraints.Offset == true
}

//CanUseClientCriteria indicates if Selector.Criteria can be used
func (v *View) CanUseClientCriteria() bool {
	return v.SelectorConstraints._criteria
}

//CanUseClientColumns indicates if Selector.Columns can be used
func (v *View) CanUseClientColumns() bool {
	return v.SelectorConstraints._columns
}

//CanUseClientLimit indicates if Selector.Limit can be used
func (v *View) CanUseClientLimit() bool {
	return v.SelectorConstraints._limit
}

//CanUseClientOrderBy indicates if Selector.OrderBy can be used
func (v *View) CanUseClientOrderBy() bool {
	return v.SelectorConstraints._orderBy
}

//CanUseClientOffset indicates if Selector.Offset can be used
func (v *View) CanUseClientOffset() bool {
	return v.SelectorConstraints._offset
}

func (v *View) filterRequiredParams() []*Parameter {
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
		result = append(result, (&v.With[i].Of.View).filterRequiredParams()...)
	}

	return result
}

func (v *View) appendReferencesParameters() {
	for _, rel := range v.With {
		(&rel.Of.View).appendReferencesParameters()
		v.mergeParams(&rel.Of.View)
	}
}

func (v *View) mergeParams(view *View) {
	v._cookiesKind.merge(view._cookiesKind)
	v._pathKind.merge(view._pathKind)
	v._headerKind.merge(view._headerKind)
	v._queryKind.merge(view._queryKind)
}
