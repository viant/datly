package state

import (
	"github.com/viant/structology"
	"sync"
)

type (
	Input struct {
		NamedStates
		mux sync.Mutex
	}

	NamedStates map[string]*structology.State
)
