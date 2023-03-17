package keywords

import (
	"github.com/viant/velty/functions"
)

type Namespace struct{}

func NewNamespace() *Namespace {
	return &Namespace{}
}

var registryInstance = functions.CopyInstance()

func AddAndGet(name string, entry *functions.Entry) string {
	return registryInstance.DefineNs(name, entry)
}

func Add(name string, entry *functions.Entry) {
	registryInstance.DefineNs(name, entry)
}
func Has(name string) bool {
	return registryInstance.IsDefined(name)
}
