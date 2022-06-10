package marshal

import (
	"github.com/viant/toolbox/format"
)

type Default struct {
	OmitEmpty  bool
	CaseFormat format.Case
	Transforms map[string]*Transform
	Exclude    map[string]bool
}

type Exclude []string

func (e Exclude) Index() map[string]bool {
	var result = map[string]bool{}
	if len(e) == 0 {
		return result
	}
	for _, item := range e {
		result[item] = true
	}
	return result

}
