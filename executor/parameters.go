package executor

import (
	"github.com/viant/datly/view"
	"github.com/viant/structology"
	"sync"
)

type Parameters struct {
	index map[string]*structology.State
	mux   sync.Mutex
}

func (p *Parameters) Add(name string, selector *view.State) {
	p.mux.Lock()
	_, ok := p.index[name]
	if !ok {
		p.index[name] = selector.State
	}
	p.mux.Unlock()
}
