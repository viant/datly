package keywords

import (
	"github.com/viant/velty/functions"
	"strings"
)

type Index map[string]bool

var ReservedKeywords = Index{}

func init() {
	ReservedKeywords.Add(functions.SlicesFunc)
	ReservedKeywords.Add(functions.MathFunc)
	ReservedKeywords.Add(functions.TimeFunc)
	ReservedKeywords.Add(functions.StringsFunc)
	ReservedKeywords.Add(functions.ErrorsFunc)
	ReservedKeywords.Add(functions.StrconvFunc)
	ReservedKeywords.Add(functions.TypesFunc)
}

func (i Index) AddAndGet(name string) string {
	i.Add(name)
	return name
}

func (i Index) Add(name string) {
	name = strings.Trim(name, "${}")
	i[name] = true
}

func (i Index) Has(name string) bool {
	_, ok := i[name]
	return ok
}
