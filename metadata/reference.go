package metadata

import (
	"encoding/json"
	"fmt"
	"github.com/viant/gtly"
	"strings"
)

type RefStrategy int

const (
	RefStrategyBinding = RefStrategy(iota)
	RefStrategySQL
)

//Reference represents  data view reference
type Reference struct {
	Name        string
	Cardinality string //One, or Many
	DataView    string
	On          []*RefMatch
	_view       *View
	_alias      string

	_refKey func(instance interface{}) interface{}
	_key    func(instance interface{}) interface{}
	_getter func(instance interface{}) interface{}
	_refIndex gtly.Index
	_index    gtly.Index
}

//SetGetter sets reference getter
func (r *Reference) SetGetter(getter func(instance interface{}) interface{}) {
	r._getter = getter
}

//RefKeyFn return reference
func (r *Reference) RefKeyFn() func(instance interface{}) interface{} {
	return r._refKey
}


//KeyFn returns key provider
func (r *Reference) KeyFn() func(instance interface{}) interface{} {
	return r._key
}

//Getter returns ref getter
func (r *Reference) Getter() func(instance interface{}) interface{} {
	return r._getter
}


//View returns association view
func (r *Reference) View() *View {
	return r._view
}

func (r *Reference) AddMatch(match *RefMatch) {
	r.On = append(r.On, match)
}

func (r *Reference) Strategy(parentConnector string) RefStrategy {
	view := r.View()
	if view == nil {
		return RefStrategyBinding
	}
	if view.Connector != parentConnector {
		return RefStrategyBinding
	}
	if len(r.On) == 0 {
		return RefStrategyBinding
	}
	for _, criteria := range r.On {
		if criteria.Param != "" {
			return RefStrategyBinding
		}
	}
	return RefStrategySQL
}

//View returns association view
func (r *Reference) SetView(view *View) {
	r._view = view
}

//Index returns index
func (r *Reference) Index() gtly.Index {
	return r._index
}

//RefIndex returns ref index
func (r *Reference) RefIndex() gtly.Index {
	return r._refIndex
}

//Alias returns alias
func (r *Reference) Alias() string {
	return r._alias
}

//RefColumns returns reference match columns
func (r *Reference) RefColumns() []string {
	var result = make([]string, 0)
	for _, on := range r.On {
		result = append(result, on.RefColumn)
	}
	return result
}

//Columns returns owner match columns
func (r *Reference) Columns() []string {
	var result = make([]string, 0)
	for _, on := range r.On {
		result = append(result, on.Column)
	}
	return result
}

//Criteria reference criteria
func (r *Reference) Criteria(alias string) string {
	var result = make([]string, 0)
	for _, on := range r.On {
		result = append(result, fmt.Sprintf("%v.%v = %v.%v", r._alias, on.Column, alias, on.RefColumn))
	}
	return strings.Join(result, " AND ")
}

func (r *Reference) MatchFromExpr(expression string) (*RefMatch, error) {
	pair := strings.SplitN(expression, "=", 2)
	if len(pair) != 2 {
		return nil, fmt.Errorf("invalid match expr: %s, expcted column=op", expression)
	}
	ref := &RefMatch{}
	value, opType := matchOperand(r._view.Table, strings.TrimSpace(pair[0]))
	ref.SetOperand(opType, value)
	value, opType = matchOperand(r._view.Table, strings.TrimSpace(pair[1]))
	ref.SetOperand(opType, value)
	return ref, nil
}

func (r *RefMatch) SetOperand(opType int, value string) {
	switch opType {
	case matchOperandTypeName:
		if r.Column == "" {
			r.Column = value
		} else {
			r.RefColumn = value
		}
	case matchOperandTypeRefName:
		r.RefColumn = value
	case matchOperandTypeParam:
		r.Param = value
	}
}

const (
	matchOperandTypeName = iota
	matchOperandTypeRefName
	matchOperandTypeParam
)

func matchOperand(refTable string, operand string) (string, int) {
	if index := strings.Index(operand, "."); index != -1 {
		if strings.HasPrefix(operand, refTable+".") {
			return operand[index+1:], matchOperandTypeRefName
		} else {
			return operand[index+1:], matchOperandTypeName
		}
	}
	if index := strings.Index(operand, ":"); index != -1 {
		return operand, matchOperandTypeParam
	}
	return operand, matchOperandTypeName
}

//Validate checks if reference is valid
func (r Reference) Validate() error {
	if r.Name == "" {
		info, _ := json.Marshal(r)
		return fmt.Errorf("reference 'name' was empty for %s", info)
	}
	if err := ValidateCardinality(r.Cardinality); err != nil {
		return fmt.Errorf("invalid reference: %v, due to: %w", r.Name, err)
	}
	if r.DataView == "" {
		return fmt.Errorf("reference 'dataView' was empty for %v", r.Name)
	}
	if len(r.On) == 0 {
		return fmt.Errorf("reference 'on' criteria was empty for %v", r.Name)
	}
	return nil
}

//RefMatch represents a column match
type RefMatch struct {
	Column    string
	RefColumn string
	Param     string
}
