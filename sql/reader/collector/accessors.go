package collector

import (
	"unsafe"

	"github.com/viant/datly/data"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

func (r *Collector) Parent() *Collector           { return r.parent }
func (r *Collector) Children() []*Collector       { return r.relations }
func (r *Collector) DataSync() *xhandler.DataSync { return r.dataSync }
func (r *Collector) View() *data.View {
	if r == nil || r.view == nil {
		return nil
	}
	return r.view.View
}
func (r *Collector) DestPtr() interface{} { return r.destValue.Interface() }
func (r *Collector) Dest() interface{}    { return r.destValue.Elem().Interface() }
func (r *Collector) ReadAll() bool        { return r.readAll }
func (r *Collector) Relation() *data.Relation {
	if r == nil || r.relation == nil {
		return nil
	}
	return r.relation.Relation
}
func (r *Collector) Slice() (unsafe.Pointer, *xunsafe.Slice) {
	return xunsafe.AsPointer(r.DestPtr()), r.slice
}

func (r *Collector) Len() int {
	if r.DestPtr() != nil && r.slice != nil {
		return r.slice.Len(xunsafe.AsPointer(r.DestPtr()))
	}
	return 0
}
