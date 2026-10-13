package handlergen

import (
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

// Context is the resolved metadata consumed by handler generation. It carries
// no authored source, discovery service, runtime state, or compiler dependency.
type Context struct {
	Component             *spec.Component
	Types                 *typecatalog.Catalog
	TypeAuthority         typecatalog.Authority
	TypeContext           *typecatalog.ResolutionContext
	ViewBindings          gen.ViewBindings
	ContractTypeOverrides ContractTypeOverrides
	GoHandler             *gen.GoHandlerAsset
	VeltyHandler          *gen.VeltyHandlerAsset
	ExternalHandler       *gen.ExternalHandler
}

// ContractTypeOverrides retains the provenance of explicitly authored names.
type ContractTypeOverrides struct{ Input, Output string }
