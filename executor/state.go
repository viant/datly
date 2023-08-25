package executor

import (
	"github.com/viant/datly/view"
	"github.com/viant/structology"
	"sync"
)

type State struct {
	index map[string]*structology.State
	mux   sync.Mutex
}

func (p *State) Add(name string, state *view.State) {
	p.mux.Lock()
	_, ok := p.index[name]
	if !ok {
		p.index[name] = state.Template
	}
	p.mux.Unlock()
}
