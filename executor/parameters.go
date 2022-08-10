package executor

import (
	"github.com/viant/datly/view"
	"sync"
)

type Parameters struct {
	index map[string]*view.ParamState
	mux   sync.Mutex
}

func (p *Parameters) Add(name string, selector *view.Selector) {
	p.mux.Lock()
	_, ok := p.index[name]
	if !ok {
		p.index[name] = &selector.Parameters
	}
	p.mux.Unlock()
}
