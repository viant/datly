package secret

import "github.com/viant/scy"

//Kind represent resource kind
type Kind string

//Resource represents secret resource
type Resource struct {
	scy.Resource
	Kind Kind
}
