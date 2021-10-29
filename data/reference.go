package data

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
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
}

//View returns association view
func (r *Reference) View() *View {
	return r._view
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

//Validate checks if reference is valid
func (r Reference) Validate() error {
	if r.Name == "" {
		info, _ := json.Marshal(r)
		return errors.Errorf("reference 'name' was empty for %s", info)
	}
	if err := ValidateCardinality(r.Cardinality); err != nil {
		return errors.Wrapf(err, "invalid reference: %v", r.Name)
	}
	if r.DataView == "" {
		return errors.Errorf("reference 'dataView' was empty for %v", r.Name)
	}
	if len(r.On) == 0 {
		return errors.Errorf("reference 'on' criteria was empty for %v", r.Name)
	}
	return nil
}

//ColumnMatch represents a column match
type ColumnMatch struct {
	Column    string
	RefColumn string
}
