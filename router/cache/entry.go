package cache

import (
	"github.com/google/uuid"
	"github.com/viant/datly/view"
)

type (
	Entry struct {
		View      *view.View
		Selectors []byte
		Data      interface{}

		result []byte
		key    string
		found  bool
	}

	Value struct {
		Selectors []byte
		Data      []byte
		ViewId    uuid.UUID
	}
)

func (e *Entry) Result() []byte {
	return e.result
}

func (e *Entry) Found() bool {
	return e.found
}
