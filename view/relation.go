package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type (
	//Relation used to build more complex View that represents database tables with relations one-to-one or many-to-many
	//In order to understand it better our example is:
	//Locators View represents Employee{AccountId: int}, Relation represents Account{Id: int}
	//We want to create result like:  Employee{Account{Id:int}}
	Relation struct {
		Name            string            `json:",omitempty"`
		Of              *ReferenceView    `json:",omitempty"`
		Caser           format.Case       `json:",omitempty"`
		Cardinality     state.Cardinality `json:",omitempty"` //IsToOne, or Many
		Column          string            `json:",omitempty"` //Represents parent column that would be used to assemble nested objects. In our example it would be Employee#AccountId
		Field           string            `json:",omitempty"` //Represents parent column that would be used to assemble nested objects. In our example it would be Employee#AccountId
		ColumnNamespace string            `json:",omitempty"` //Represents column namespace, can be specified if $shared.Criteria / $shared.ColumnInPosition is inside the "from" statement
		Holder          string            `json:",omitempty"` //Represents column created due to the merging. In our example it would be Employee#Account
		IncludeColumn   bool              `json:",omitempty"` //tells if Column _field should be kept in the struct type. In our example, if set false in produced Employee would be also AccountId _field

		hasColumnField bool
		holderField    *xunsafe.Field
		columnField    *xunsafe.Field
	}

	//ReferenceView represents referenced View
	//In our example it would be Account
	ReferenceView struct {
		View          // event type
		Column string `json:",omitempty"`
		Field  string `json:",omitempty"`
		_field *xunsafe.Field
	}
)

// Init initializes ReferenceView
func (r *ReferenceView) Init(_ context.Context, _ *Resource) error {
	r.initializeField()
	return r.Validate()
}

func (r *Relation) inheritType(rType reflect.Type) error {
	r.Of.Schema.InheritType(rType)
	r.Of.initializeField()
	if err := r.Of.View.deriveColumnsFromSchema(r); err != nil {
		return err
	}
	return nil
}

func (r *ReferenceView) initializeField() {
	if r.Field == "" {
		r.Field = r.Column
	}
	r._field = shared.MatchField(r.Schema.Type(), r.Field, r.Caser)
}

// Validate checks if ReferenceView is valid
func (r *ReferenceView) Validate() error {
	if r.Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

// Init initializes Relation
func (r *Relation) Init(ctx context.Context, parent *View) error {
	if r.Field == "" {
		r.Field = r.Column
	}

	field := shared.MatchField(parent.DataType(), r.Holder, r.Of.View.Caser)

	if err := r.inheritType(field.Type); err != nil {
		return err
	}

	if err := r.initHolder(parent); err != nil {
		return err
	}

	view := &r.Of.View
	view.updateColumnTypes()

	if err := view.indexSqlxColumnsByFieldName(); err != nil {
		return err
	}

	view.indexColumns()

	return r.Validate()
}

func (r *Relation) initHolder(v *View) error {
	dataType := v.DataType()
	r.holderField = shared.MatchField(dataType, r.Holder, v.Caser)
	if r.holderField == nil {
		return fmt.Errorf("failed to lookup holderField %v", r.Holder)
	}

	columnName := r.Of.Caser.Format(r.Field, format.CaseUpperCamel)
	r.columnField = shared.MatchField(v.DataType(), columnName, v.Caser)

	r.hasColumnField = r.columnField != nil
	if r.Cardinality == state.Many && !r.hasColumnField {
		return fmt.Errorf("column %v doesn't have corresponding _field in the struct: %v", columnName, v.DataType().String())
	}

	return nil
}

// Validate checks if Relation is valid
func (r *Relation) Validate() error {
	if r.Cardinality != state.Many && r.Cardinality != state.One {
		return fmt.Errorf("cardinality has to be Many or IsToOne")
	}

	if r.Column == "" {
		return fmt.Errorf("column can't be empty")
	}

	if r.Of == nil {
		return fmt.Errorf("relation of can't be nil")
	}
	if err := r.Of.Validate(); err != nil {
		return err
	}
	if r.Holder == "" {
		return fmt.Errorf("refHolder can't be empty")
	}

	if strings.Title(r.Holder)[0] != r.Holder[0] {
		return fmt.Errorf("holder has to start with uppercase")
	}

	return nil
}

func (r *Relation) ensureColumnAliasIfNeeded() error {
	columnSegments := strings.Split(r.Column, ".")
	if len(columnSegments) > 2 {
		return fmt.Errorf("invalid column name, supported only 0 or 1 dots are allowed")
	}

	if len(columnSegments) == 1 {
		return nil
	}

	r.Column = columnSegments[1]
	r.ColumnNamespace = columnSegments[0]

	return nil
}

// ViewReference creates a View reference
func ViewReference(name, ref string, options ...Option) *View {
	viewRef := &View{
		Name:      name,
		Reference: shared.Reference{Ref: ref},
	}

	viewRef.applyOptions(options)

	return viewRef
}

func (v *View) applyOptions(options []Option) {
	for _, option := range options {
		switch actual := option.(type) {
		case logger.Logger:
			v.Logger = logger.NewLogger("", actual)
		case logger.Counter:
			v.Counter = actual
		}
	}
}

// RelationsSlice represents slice of Relation
type RelationsSlice []*Relation

// Index indexes Relations by Relation.Holder
func (r RelationsSlice) Index() map[string]*Relation {
	result := make(map[string]*Relation)
	for i, rel := range r {
		keys := shared.KeysOf(rel.Holder, true)

		for _, key := range keys {
			result[key] = r[i]
		}
	}

	return result
}

// PopulateWithResolve filters RelationsSlice by the columns that won't be present in Database
// due to the removing StructField after assembling nested StructType.
func (r RelationsSlice) PopulateWithResolve() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if !rel.hasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}

// Columns extract Relation.Column from RelationsSlice
func (r RelationsSlice) Columns() []string {
	resolverColumns := make([]string, 0)
	for i := range r {
		resolverColumns = append(resolverColumns, r[i].Column)
	}
	return resolverColumns
}

// PopulateWithVisitor filters RelationsSlice by the columns that will be present in Database, and because of that
// they wouldn't be resolved as unmapped columns.
func (r RelationsSlice) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if rel.hasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}

// NwReferenceView creates a reference View
func NwReferenceView(field, column string, view *View) *ReferenceView {
	return &ReferenceView{View: *view, Column: column, Field: field}
}
