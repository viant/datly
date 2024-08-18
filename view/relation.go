package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"strings"
)

type (
	//Relation used to build more complex View that represents database tables with relations one-to-one or many-to-many
	//In order to understand it better our example is:
	//locators View represents Employee{AccountId: int}, Relation represents Account{Id: int}
	//We want to create result like:  Employee{Account{Id:int}}
	Relation struct {
		Name          string            `json:",omitempty"`
		Of            *ReferenceView    `json:",omitempty"`
		Caser         text.CaseFormat   `json:",omitempty"`
		Cardinality   state.Cardinality `json:",omitempty"` //IsToOne, or Many
		On            Links
		Holder        string `json:",omitempty"` //Represents column created due to the merging. In our example it would be Employee#Account
		IncludeColumn bool   `json:",omitempty"` //tells if Column _field should be kept in the struct type. In our example, if set false in produced Employee would be also AccountId _field
		holderField   *xunsafe.Field
	}

	//ReferenceView represents referenced View
	//In our example it would be Account
	ReferenceView struct {
		View // event type
		//Link
		On Links `json:",omitempty"`
	}

	Links []*Link

	Link struct {
		Namespace     string
		Column        string
		Field         string
		IncludeColumn *bool
		xField        *xunsafe.Field
	}
)

// JoinOn returns links
func JoinOn(links ...*Link) Links {
	return links
}

// WithLink returns a link
func WithLink(field, column string) *Link {
	l := &Link{Field: field, Column: column}
	l.ensureNamespace()
	return l
}

func (l Links) Init(name string, v *View) error {
	rType := v.DataType()
	for _, link := range l {
		link.Init()
		if link.Namespace == "" {
			//link.Namespace = v.Alias
		}
		if link.Field != "" {
			if link.xField = shared.MatchField(rType, link.Field, v.CaseFormat); link.xField == nil {
				return fmt.Errorf("invalid relation %v, field %v not defined in the struct: %s", name, link.Field, v.DataType().String())
			}
		}
		if link.Field == "" && link.Column != "" {
			derivedField := v.CaseFormat.Format(link.Column, text.CaseFormatUpperCamel)
			link.xField = shared.MatchField(v.DataType(), derivedField, v.CaseFormat)
		}
	}
	return nil
}

func (l *Link) Validate() error {
	if l.Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

func (l Links) InColumnExpression() []string {
	var ret []string
	for _, link := range l {
		if link.Namespace != "" {
			ret = append(ret, link.Namespace+"."+link.Column)
			continue
		}
		ret = append(ret, link.Column)
	}
	return ret
}

func (l Links) Validate() error {
	for _, link := range l {
		if err := link.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (l Links) Namespace() string {
	for _, link := range l {
		if link.Namespace != "" {
			return link.Namespace
		}
	}
	return ""
}

func (l *Link) Init() {
	l.ensureNamespace()
}

func (l *Link) ensureNamespace() {
	if l.Namespace != "" {
		return
	}
	l.Column, l.Namespace = extractNamesapce(l.Column)
}

func extractNamesapce(column string) (string, string) {
	ns := ""
	if index := strings.Index(column, "."); index != -1 {
		ns = column[:index]
		column = column[index+1:]
	}
	return column, ns
}

// Init initializes ReferenceView
func (r *ReferenceView) Init(_ context.Context, aView *View) (err error) {
	if err = r.On.Init(r.Name, aView); err != nil {
		return err
	}
	return r.On.Validate()
}

func (r *Relation) inheritType() {
	if r.Of.Schema != nil && r.Of.Schema.Type() != nil {
		return
	}
	if r.Of.Schema == nil {
		r.Of.Schema = &state.Schema{}
	}
	r.Of.Schema.InheritType(r.holderField.Type)
}

// Validate checks if ReferenceView is valid
func (r *ReferenceView) Validate() error {
	if r.On[0].Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

// Init initializes Relation
func (r *Relation) Init(ctx context.Context, parent *View) error {
	if err := r.initParentLink(parent); err != nil {
		return err
	}
	r.holderField = shared.MatchField(parent.DataType(), r.Holder, r.Of.View.CaseFormat)
	if r.holderField == nil {
		return fmt.Errorf("failed to lookup holderField %v", r.Holder)
	}
	r.inheritType()
	view := &r.Of.View
	if err := r.Of.Init(ctx, view); err != nil {
		return err
	}

	//TODO analyze if still needed, given column are inherited from schema or schema is ingerited from columns
	view.updateColumnTypes()
	if err := view.indexSqlxColumnsByFieldName(); err != nil {
		return err
	}
	view.indexColumns()
	return r.Validate()
}

func (r *Relation) initParentLink(v *View) error {
	return r.On.Init(r.Name, v)
}

// Validate checks if Relation is valid
func (r *Relation) Validate() error {
	if r.Cardinality != state.Many && r.Cardinality != state.One {
		return fmt.Errorf("cardinality has to be Many or IsToOne")
	}

	if r.On[0].Column == "" {
		return fmt.Errorf("relation column can't be empty")
	}

	if r.Of == nil {
		return fmt.Errorf("relation of can't be nil")
	}
	if err := r.Of.On.Validate(); err != nil {
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

func (r *Relation) adjustLinkColumn() error {
	byName := Columns(r.Of.View.Columns).Index(text.CaseFormatLower)

	for i, link := range r.Of.On {
		if link.Column == "" {
			continue
		}
		if _, ok := byName[strings.ToLower(link.Column)]; ok {
			continue
		}

		if i >= len(r.On) {
			return fmt.Errorf("invalid on link %s", r.Name)
		}

		parentLink := r.On[i]
		columnType := xreflect.InterfaceType
		if parentLink.xField != nil {
			columnType = parentLink.xField.Type
		}

		if link.xField != nil {
			columnType = link.xField.Type
		}
		relColumn := &Column{Name: link.Column, Expression: link.Column}
		relColumn.SetColumnType(columnType)
		if err := relColumn.Init(r.Of.View.Resource(), r.Of.View.CaseFormat, false); err != nil {
			return err
		}
		r.Of.View.Columns = append(r.Of.View.Columns, relColumn)

	}
	return nil
}

func (r *Relation) TagLink() tags.LinkOn {
	var links []string
	for i, parent := range r.On {
		child := r.Of.On[i]
		links = append(links, parent.EncodeLinkTag()+"="+child.EncodeLinkTag())
	}
	return links
}

func (l *Link) EncodeLinkTag() string {
	result := ""
	if l.Field != "" {
		result += l.Field
	}
	if result != "" {
		result += ":"
	}
	if l.Namespace != "" && l.Column != "" {
		result += l.Namespace + "."
	}
	if l.Column != "" {
		result += l.Column
	}
	return result
}

// RelationsSlice represents slice of Relation
type RelationsSlice []*Relation

// PopulateWithVisitor filters RelationsSlice by the columns that will be present in Database, and because of that
// they wouldn't be resolved as unmapped columns.
func (r RelationsSlice) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0)
	for i, _ := range r {
		result = append(result, r[i])
	}
	return result
}

// NewReferenceView creates a reference View
func NewReferenceView(links Links, view *View) *ReferenceView {
	return &ReferenceView{View: *view, On: links}
}
