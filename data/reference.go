package data

import (
	"datly/base"
	"datly/generic"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"strings"
)

//Reference represents  data view reference
type Reference struct {
	Name        string
	Cardinality string //One, or Many
	DataView    string
	On          []*ColumnMatch
	_view       *View
	_alias      string
	_refIndex   generic.Index
	_index      generic.Index
}

//View returns association view
func (a *Reference) View() *View {
	return a._view
}

//Index returns index
func (a *Reference) Index() generic.Index {
	return a._index
}

//RefIndex returns ref index
func (a *Reference) RefIndex() generic.Index {
	return a._refIndex
}

//Alias returns alias
func (a *Reference) Alias() string {
	return a._alias
}

//RefColumns returns reference match columns
func (a *Reference) RefColumns() []string {
	var result = make([]string, 0)
	for _, on := range a.On {
		result = append(result, on.RefColumn)
	}
	return result
}

//Columns returns owner match columns
func (a *Reference) Columns() []string {
	var result = make([]string, 0)
	for _, on := range a.On {
		result = append(result, on.Column)
	}
	return result
}

//Criteria reference criteria
func (a *Reference) Criteria(alias string) string {
	var result = make([]string, 0)
	for _, on := range a.On {
		result = append(result, fmt.Sprintf("%v.%v = %v.%v", a._alias, on.Column, alias, on.RefColumn))
	}
	return strings.Join(result, " AND ")
}

//Validate checks if reference is valid
func (a Reference) Validate() error {
	if a.Name == "" {
		info, _ := json.Marshal(a)
		return errors.Errorf("reference 'name' was empty for %s", info)
	}
	switch a.Cardinality {
	case base.CardinalityMany, base.CardinalityOne:
	default:
		return errors.Errorf("unsupported reference cardinality: '%s', supported: %v, %v", a.Cardinality, a.Name, base.CardinalityMany, base.CardinalityOne)
	}
	if a.DataView == "" {
		return errors.Errorf("reference 'dataView' was empty for %v", a.Name)
	}
	if len(a.On) == 0 {
		return errors.Errorf("reference 'on' criteria was empty for %v", a.Name)
	}
	return nil
}

//ColumnMatch represents a column match
type ColumnMatch struct {
	Column    string
	RefColumn string
}
