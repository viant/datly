package collector

import (
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/viant/xunsafe"
)

func (r *Collector) SetDest(dest interface{}) {
	if r.provenance {
		r.rowEvidence = nil
	}
	r.setDest(dest)
	if r.provenance {
		for i := 0; i < r.Len(); i++ {
			r.evidence(i)
		}
	}
}

func (r *Collector) setDest(dest interface{}) {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() == reflect.Ptr {
		r.destValue.Elem().Set(destValue.Elem())
	} else {
		r.destValue.Elem().Set(destValue)
	}
}

func (r *Collector) Clone() *Collector {
	slicePtrValue := reflect.New(r.view.Schema.SliceType())
	dest := reflect.MakeSlice(r.view.Schema.SliceType(), 0, 1)
	slicePtrValue.Elem().Set(dest)
	return &Collector{
		Id:                     uuid.New().String(),
		indexMutex:             r.indexMutex,
		parent:                 r.parent,
		destValue:              slicePtrValue,
		appender:               r.slice.Appender(xunsafe.ValuePointer(&slicePtrValue)),
		valuePosition:          r.valuePosition,
		compositeValuePosition: r.compositeValuePosition,
		preparedValuePosition:  r.preparedValuePosition,
		preparedComposite:      r.preparedComposite,
		types:                  r.types,
		relation:               r.relation,
		values:                 r.values,
		slice:                  r.slice,
		view:                   r.view,
		relations:              r.relations,
		dataSync:               r.dataSync,
		wg:                     r.wg,
		readAll:                r.readAll,
		wgDelta:                r.wgDelta,
		indexCounter:           r.indexCounter,
		provenance:             r.provenance,
	}
}

func (r *Collector) Lock() *sync.Mutex {
	if r.parent == nil {
		return &r.mutex
	}
	return &r.parent.mutex
}
