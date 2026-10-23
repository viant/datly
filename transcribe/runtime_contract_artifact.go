package transcribe

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	x "github.com/viant/x"
)

// RuntimeContractArtifactVersion identifies the portable dynamic reader format.
const RuntimeContractArtifactVersion = "datly.runtime-contract/v2"

// ReadRuntimeContractArtifactFS reads one exact, content-addressed compiler
// artifact. The caller owns source/input-hash selection and resource authority;
// this reader never searches for a newer artifact or invokes discovery.
func ReadRuntimeContractArtifactFS(folder fs.FS, path, expectedSHA256 string) (*RuntimeContractArtifact, error) {
	if folder == nil || path == "." || !fs.ValidPath(path) || len(expectedSHA256) != 64 {
		return nil, fmt.Errorf("artifact filesystem, path, and SHA-256 are required")
	}
	raw, err := fs.ReadFile(folder, path)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != expectedSHA256 {
		return nil, fmt.Errorf("compiled artifact digest mismatch")
	}
	var artifact RuntimeContractArtifact
	if err = json.Unmarshal(raw, &artifact); err != nil {
		return nil, err
	}
	if artifact.Version != RuntimeContractArtifactVersion {
		return nil, fmt.Errorf("unsupported runtime contract artifact version")
	}
	return &artifact, nil
}

// RuntimeContractArtifact is a resolved dynamic reader, without compiler state,
// database handles, or executable code. Resources remain host-owned: callers must
// supply the exact immutable resource generation used at publication. Persist
// artifacts together with the host's source/config/resource revision identity.
// Treat this format as trusted publication output, not client-authored metadata.
type RuntimeContractArtifact struct {
	Version           string                       `json:"version"`
	Component         *spec.Component              `json:"component"`
	Input             string                       `json:"input"`
	Output            string                       `json:"output"`
	Types             []RuntimeArtifactType        `json:"types"`
	Catalog           []RuntimeArtifactCatalogType `json:"catalog,omitempty"`
	ResourcesRequired bool                         `json:"resourcesRequired,omitempty"`
}

// RuntimeArtifactCatalogType retains both effective catalog authorities. Named
// identities are linked from the host; generated structural identities are rebuilt.
type RuntimeArtifactCatalogType struct {
	Package string                 `json:"package,omitempty"`
	Name    string                 `json:"name"`
	Origin  typecatalog.TypeOrigin `json:"origin"`
	Type    string                 `json:"type"`
}

// Freeze captures already resolved types and metadata. It never compiles DQL or
// discovers database schemas. Source-only catalog descriptors and executable
// handlers cannot be persisted by this reader format and fail explicitly.
func (c *RuntimeContract) Freeze() (*RuntimeContractArtifact, error) {
	if c == nil || c.Component == nil || c.InputType == nil || c.OutputType == nil {
		return nil, fmt.Errorf("resolved runtime contract is required")
	}
	if err := artifactReader(c.Component); err != nil {
		return nil, err
	}
	if c.Component.ReaderGraph != nil || c.Component.SelectorCompanion != nil {
		return nil, fmt.Errorf("native holder authority cannot be persisted as a dynamic reader")
	}
	if c.Plan != nil && (c.Plan.GoHandler != nil || c.Plan.ExternalHandler != nil || c.Plan.ContractHandler != nil || c.Plan.MutationHandler != nil || c.Plan.VeltyHandler != nil || c.Plan.FactoryLink != nil) {
		return nil, fmt.Errorf("executable handler contracts cannot be persisted")
	}
	a := &RuntimeContractArtifact{Version: RuntimeContractArtifactVersion, ResourcesRequired: c.Resources != nil}
	data, err := json.Marshal(c.Component)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &a.Component); err != nil {
		return nil, err
	}
	encoder := artifactTypeEncoder{ids: map[reflect.Type]string{}, artifact: a}
	if a.Input, err = encoder.add(c.InputType); err != nil {
		return nil, err
	}
	if a.Output, err = encoder.add(c.OutputType); err != nil {
		return nil, err
	}
	if c.Types != nil {
		snapshot, err := c.Types.Clone()
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
			registry, e := snapshot.Registry(authority)
			if e != nil {
				return nil, e
			}
			for _, key := range registry.Keys() {
				descriptor := registry.Lookup(key)
				origin, _, e := snapshot.Origin(authority, key)
				if e != nil {
					return nil, e
				}
				identity := string(origin) + ":" + key
				if seen[identity] {
					continue
				}
				seen[identity] = true
				if descriptor.Type == nil || descriptor.SynteticType != nil || descriptor.Definition != "" {
					return nil, fmt.Errorf("catalog type %q has source descriptor metadata unsupported by runtime artifact v1", key)
				}
				id, e := encoder.add(descriptor.Type)
				if e != nil {
					return nil, fmt.Errorf("catalog type %s: %w", key, e)
				}
				a.Catalog = append(a.Catalog, RuntimeArtifactCatalogType{Package: descriptor.PkgPath, Name: descriptor.Name, Origin: origin, Type: id})
			}
		}
	}
	return a, nil
}

// Materialize reconstructs a resolved reader using only in-memory type/resource
// authorities. It never reads Go source, compiles DQL, or contacts a database.
// Result and Plan intentionally remain nil: they are authoring products.
func (a *RuntimeContractArtifact) Materialize(seed *typecatalog.Catalog, resources *resource.Store) (_ *RuntimeContract, err error) {
	if a == nil || a.Version != RuntimeContractArtifactVersion {
		return nil, fmt.Errorf("unsupported runtime contract artifact version")
	}
	if a.Component == nil {
		return nil, fmt.Errorf("runtime artifact component is required")
	}
	if err = artifactReader(a.Component); err != nil {
		return nil, err
	}
	if a.Component.ReaderGraph != nil || a.Component.SelectorCompanion != nil {
		return nil, fmt.Errorf("runtime artifact contains transient native holder authority")
	}
	if a.ResourcesRequired && resources == nil {
		return nil, fmt.Errorf("runtime artifact requires its immutable resource generation")
	}
	catalog := typecatalog.NewCatalog()
	if seed != nil {
		if catalog, err = seed.Clone(); err != nil {
			return nil, err
		}
	}
	decoder := artifactTypeDecoder{catalog: catalog, nodes: map[string]RuntimeArtifactType{}, types: map[string]reflect.Type{}, building: map[string]bool{}}
	for _, node := range a.Types {
		if node.ID == "" {
			return nil, fmt.Errorf("artifact type ID is required")
		}
		if _, exists := decoder.nodes[node.ID]; exists {
			return nil, fmt.Errorf("duplicate artifact type %q", node.ID)
		}
		decoder.nodes[node.ID] = node
	}
	// Validate every type, including nodes not reachable from the input/output.
	for _, node := range a.Types {
		if _, err = decoder.materialize(node.ID); err != nil {
			return nil, err
		}
	}
	expectedCatalog := typecatalog.NewCatalog()
	for _, binding := range a.Catalog {
		if strings.TrimSpace(binding.Name) == "" {
			return nil, fmt.Errorf("artifact catalog type name is required")
		}
		typ, e := decoder.materialize(binding.Type)
		if e != nil {
			return nil, e
		}
		descriptor := x.NewType(typ, x.WithPkgPath(binding.Package), x.WithName(binding.Name))
		key := descriptor.Key()
		if e = expectedCatalog.Register(binding.Origin, descriptor); e != nil {
			return nil, e
		}
		prior, found, e := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, key)
		if e != nil {
			return nil, e
		}
		origin, _, e := catalog.Origin(typecatalog.PackageAuthority, key)
		if e != nil {
			return nil, e
		}
		if found && origin == binding.Origin {
			if prior != typ {
				return nil, fmt.Errorf("artifact catalog type %q conflicts with host identity", key)
			}
			continue
		}
		if e = catalog.Register(binding.Origin, descriptor); e != nil {
			return nil, e
		}
	}
	// A host seed may supply linked identities, but cannot change the published
	// ownership selected for a catalog key by either authority.
	for _, binding := range a.Catalog {
		key := binding.Name
		if binding.Package != "" {
			key = binding.Package + "." + key
		}
		for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
			wantType, _, e := expectedCatalog.ResolveRuntimeType(authority, key)
			if e != nil {
				return nil, e
			}
			gotType, _, e := catalog.ResolveRuntimeType(authority, key)
			if e != nil {
				return nil, e
			}
			wantOrigin, _, e := expectedCatalog.Origin(authority, key)
			if e != nil {
				return nil, e
			}
			gotOrigin, _, e := catalog.Origin(authority, key)
			if e != nil {
				return nil, e
			}
			if wantType != gotType || wantOrigin != gotOrigin {
				return nil, fmt.Errorf("host catalog changes published %s authority for %q", authority, key)
			}
		}
	}
	input, err := decoder.materialize(a.Input)
	if err != nil {
		return nil, err
	}
	output, err := decoder.materialize(a.Output)
	if err != nil {
		return nil, err
	}
	// Clone through the wire representation so callers cannot mutate a running
	// generation through the artifact's component graph.
	raw, err := json.Marshal(a.Component)
	if err != nil {
		return nil, err
	}
	component := &spec.Component{}
	if err = json.Unmarshal(raw, component); err != nil {
		return nil, err
	}
	return &RuntimeContract{Component: component, InputType: input, OutputType: output, Resources: resources, Types: catalog}, nil
}

func artifactReader(component *spec.Component) error {
	if component.Settings != nil && strings.TrimSpace(component.Settings.Mutation) != "" {
		return fmt.Errorf("runtime artifact v1 supports readers only")
	}
	for _, route := range component.Routes {
		if route == nil {
			return fmt.Errorf("runtime artifact has a nil route")
		}
		switch strings.ToUpper(route.Method) {
		case "GET", "POST", "HEAD":
		default:
			return fmt.Errorf("runtime artifact v1 does not support route method %q", route.Method)
		}
		if route.Handler != "" {
			return fmt.Errorf("runtime artifact v1 does not support executable route handlers")
		}
	}
	return nil
}

// Hash returns a deterministic content identity for the resolved artifact.
func (a *RuntimeContractArtifact) Hash() (string, error) {
	raw, err := json.Marshal(a)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
