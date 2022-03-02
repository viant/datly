package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type (
	Cardinality string
	//Relation used to build more complex View that represents database tables with relations one-to-one or many-to-many
	//In order to understand it better our example is:
	//Parent View represents Employee{AccountId: int}, Relation represents Account{Id: int}
	//We want to create result like:  Employee{Account{Id:int}}
	Relation struct {
		Name string
		Of   *ReferenceView

		Cardinality   Cardinality //One, or Many
		Column        string      //Represents parent column that would be used to assemble nested objects. In our example it would be Employee#AccountId
		Holder        string      //Represents column created due to the merging. In our example it would be Employee#Account
		IncludeColumn bool        //tells if Column field should be kept in the struct type. In our example, if set false in produced Employee would be also AccountId field

		hasColumnField bool
		holderField    *xunsafe.Field
		columnField    *xunsafe.Field
	}

	//ReferenceView represents referenced View
	//In our example it would be Account
	ReferenceView struct {
		View          // event type
		Column string // EventType.id
		field  *xunsafe.Field
	}
)

const (
	One  Cardinality = "One"
	Many Cardinality = "Many"
)

//Init initializes ReferenceView
func (r *ReferenceView) Init(ctx context.Context, resource *Resource) error {
	r.initializeField()
	return r.Validate()
}

func (r *Relation) inheritType(rType reflect.Type) error {
	r.Of.Schema.inheritType(rType)
	r.Of.initializeField()
	if err := r.Of.View.deriveColumnsFromSchema(); err != nil {
		return err
	}
	return nil
}

func (r *ReferenceView) initializeField() {
	rType := deref(r.Schema.Type())
	r.field = shared.MatchField(rType, r.Column, r.Caser)
}

//Validate checks if ReferenceView is valid
func (r *ReferenceView) Validate() error {
	if r.Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

//Init initializes Relation
func (r *Relation) Init(ctx context.Context, resource *Resource, parent *View) error {
	if err := r.initHolder(parent); err != nil {
		return err
	}

	if err := r.inheritType(r.holderField.Type); err != nil {
		return err
	}

	return r.Validate()
}

func (r *Relation) initHolder(v *View) error {
	dataType := v.DataType()
	r.holderField = shared.MatchField(dataType, r.Holder, v.Caser)
	if r.holderField == nil {
		return fmt.Errorf("failed to lookup holderField %v", r.Holder)
	}

	columnName := v.Caser.Format(r.Column, format.CaseUpperCamel)
	r.columnField = shared.MatchField(v.DataType(), columnName, v.Caser)

	r.hasColumnField = r.columnField != nil
	if r.Cardinality == Many && !r.hasColumnField {
		return fmt.Errorf("column %v doesn't have corresponding field in the struct: %v", columnName, v.DataType().String())
	}

	return nil
}

//Validate checks if Relation is valid
func (r *Relation) Validate() error {
	if r.Cardinality != Many && r.Cardinality != One {
		return fmt.Errorf("cardinality has to be Many or One")
	}

	if r.Column == "" {
		return fmt.Errorf("column can't be empty")
	}

	if r.Of == nil {
		return fmt.Errorf("relation of can't be nil")
	}

	if r.Holder == "" {
		return fmt.Errorf("refHolder can't be empty")
	}

	if strings.Title(r.Holder)[0] != r.Holder[0] {
		return fmt.Errorf("holder has to start with uppercase")
	}

	return nil
}
