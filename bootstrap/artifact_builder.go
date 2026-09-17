package bootstrap

import (
	"fmt"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// ArtifactBuilder owns one detached native registry snapshot for a complete
// compilation stage. Reuse it for every component in that stage; later export
// registrations cannot change its selected types or compiled factories.
type ArtifactBuilder struct{ registry *x.Registry }

func NewArtifactBuilder(registry *x.Registry) (*ArtifactBuilder, error) {
	result := &ArtifactBuilder{}
	if registry != nil {
		snapshot, err := (x.Cloner{}).Registry(registry)
		if err != nil {
			return nil, err
		}
		result.registry = snapshot
	}
	return result, nil
}

func (b *ArtifactBuilder) Build(input ArtifactInput) (*Artifact, error) {
	if b == nil {
		return nil, fmt.Errorf("artifact builder is required")
	}
	if b.registry != nil {
		var err error
		input.Types, err = b.Catalog(input.Types)
		if err != nil {
			return nil, err
		}
	}
	artifact, err := (&artifactCompiler{input: input}).compile()
	if err != nil {
		return nil, err
	}
	// A handler discovered from the compiled package holder is direct linked
	// authority. The dynamic registry is only a fallback for runtime-defined
	// components; it must not replace or re-register a linked factory.
	if artifact.Handler == nil && b.registry != nil {
		artifact.Handler, err = b.handler(artifact)
		if err != nil {
			return nil, err
		}
	}
	return artifact, nil
}

// Catalog returns detached type authority including linked registry types from
// this same snapshot, for publication alongside the completed artifacts.
func (b *ArtifactBuilder) Catalog(source *typecatalog.Catalog) (*typecatalog.Catalog, error) {
	if b == nil {
		return nil, fmt.Errorf("artifact builder is required")
	}
	var result *typecatalog.Catalog
	var err error
	if source == nil {
		result = typecatalog.NewCatalog()
	} else {
		result, err = source.Clone()
		if err != nil {
			return nil, err
		}
	}
	if b.registry != nil {
		var descriptors []*x.Type
		var lookupErr error
		b.registry.ForEach(func(_ string, typ *x.Type) bool {
			existing, found, err := result.Resolve(typecatalog.PackageAuthority, typ.Key())
			if err != nil {
				lookupErr = err
				return false
			}
			// Discovery may already have attached this exact compiled type to its
			// source descriptor. Retain the source metadata instead of replacing it.
			if found && existing.Type != nil && existing.Type == typ.Type {
				return true
			}
			descriptors = append(descriptors, typ)
			return true
		})
		if lookupErr != nil {
			return nil, lookupErr
		}
		if err = result.RegisterAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (b *ArtifactBuilder) handler(artifact *Artifact) (rhandler.TypedHandler, error) {
	linker := handlerLinker{artifact: artifact, registry: b.registry}
	key, err := linker.reference()
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, nil
	}
	return linker.resolve(key)
}
