package data

import (
	"github.com/viant/gtly"
)

//Reference represents  data View reference
type Reference struct {
	Name        string
	Cardinality string //One, or Many
	On          *ColumnMatch

	_view     *View
	_alias    string
	_refIndex gtly.Index
	_index    gtly.Index
}

//View returns association View
func (r *Reference) View() *View {
	return r._view
}

//Index returns index
func (r *Reference) Index() gtly.Index {
	return r._index
}

//RefIndex returns Ref index
func (r *Reference) RefIndex() gtly.Index {
	return r._refIndex
}

//Alias returns alias
func (r *Reference) Alias() string {
	return r._alias
}

//ColumnMatch represents a column match
type ColumnMatch struct {
	Column    string
	RefColumn string
	RefHolder string
}
