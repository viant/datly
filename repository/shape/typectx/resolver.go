package typectx

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/viant/x"
)

// AmbiguityError reports multiple matching type candidates for a type expression.
type AmbiguityError struct {
	Expression string
	Candidates []string
}

func (e *AmbiguityError) Error() string {
	return fmt.Sprintf("ambiguous type %q: candidates=%s", e.Expression, strings.Join(e.Candidates, ","))
}

// Resolver resolves cast/tag type expressions against viant/x registry using type context.
type Resolver struct {
	registry   *x.Registry
	context    *Context
	provenance map[string]Provenance
}

// NewResolver creates a type resolver.
func NewResolver(registry *x.Registry, context *Context) *Resolver {
	return NewResolverWithProvenance(registry, context, nil)
}

// NewResolverWithProvenance creates a type resolver with optional registry-key provenance map.
func NewResolverWithProvenance(registry *x.Registry, context *Context, provenance map[string]Provenance) *Resolver {
	return &Resolver{
		registry:   registry,
		context:    normalizeContext(context),
		provenance: cloneProvenance(provenance),
	}
}

// Resolve resolves type expression to registry key. It returns ("", nil) when unresolved.
func (r *Resolver) Resolve(typeExpr string) (string, error) {
	resolved, err := r.ResolveWithProvenance(typeExpr)
	if err != nil || resolved == nil {
		return "", err
	}
	return resolved.ResolvedKey, nil
}

// ResolveWithProvenance resolves expression and returns provenance details.
// It returns (nil, nil) when unresolved.
func (r *Resolver) ResolveWithProvenance(typeExpr string) (*Resolution, error) {
	if r == nil || r.registry == nil {
		return nil, nil
	}
	base := normalizeLookupKey(typeExpr)
	if base == "" {
		return nil, nil
	}

	// Exact type key (builtins or fully-qualified package.Type)
	if r.registry.Lookup(base) != nil {
		return r.newResolution(typeExpr, "", base, "exact"), nil
	}

	prefix, baseName, alias, qualified := splitQualified(base)
	if qualified {
		if prefix == "" || baseName == "" {
			return nil, nil
		}
		if alias {
			pkg := r.aliasPackage(prefix)
			if pkg == "" {
				return nil, nil
			}
			candidate := pkg + "." + baseName
			if r.registry.Lookup(candidate) == nil {
				return nil, nil
			}
			return r.newResolution(typeExpr, "", candidate, "alias_import"), nil
		}
		// fully qualified package path.Type
		if r.registry.Lookup(base) != nil {
			return r.newResolution(typeExpr, "", base, "qualified"), nil
		}
		return nil, nil
	}

	// Unqualified resolution: default package, then imports; if still unresolved,
	// fallback to unique global name match.
	candidates := r.unqualifiedCandidates(baseName)
	if len(candidates) == 1 {
		return r.newResolution(typeExpr, "", candidates[0].key, candidates[0].matchKind), nil
	}
	if len(candidates) > 1 {
		keys := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			keys = append(keys, candidate.key)
		}
		sort.Strings(keys)
		return nil, &AmbiguityError{Expression: typeExpr, Candidates: keys}
	}
	return nil, nil
}

func (r *Resolver) aliasPackage(alias string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" || r.context == nil {
		return ""
	}
	for _, item := range r.context.Imports {
		if item.Alias == alias {
			return item.Package
		}
	}
	return ""
}

type candidate struct {
	key       string
	matchKind string
}

func (r *Resolver) unqualifiedCandidates(typeName string) []candidate {
	if typeName == "" {
		return nil
	}
	seen := map[string]bool{}
	var result []candidate

	for _, scoped := range r.searchPackages() {
		pkg := scoped.pkg
		key := pkg + "." + typeName
		if seen[key] {
			continue
		}
		seen[key] = true
		if r.registry.Lookup(key) != nil {
			result = append(result, candidate{key: key, matchKind: scoped.matchKind})
		}
	}
	if len(result) > 0 {
		return result
	}

	// Global unique fallback by suffix ".TypeName" or exact built-in.
	for _, key := range r.registry.Keys() {
		if key == typeName || strings.HasSuffix(key, "."+typeName) {
			if seen[key] {
				continue
			}
			seen[key] = true
			result = append(result, candidate{key: key, matchKind: "global_unique"})
		}
	}
	return result
}

type scopedPackage struct {
	pkg       string
	matchKind string
}

func (r *Resolver) searchPackages() []scopedPackage {
	if r.context == nil {
		return nil
	}
	seen := map[string]bool{}
	var result []scopedPackage
	appendPkg := func(pkg, matchKind string) {
		pkg = strings.TrimSpace(pkg)
		if pkg == "" || seen[pkg] {
			return
		}
		seen[pkg] = true
		result = append(result, scopedPackage{pkg: pkg, matchKind: matchKind})
	}
	appendPkg(r.context.DefaultPackage, "default_package")
	for _, item := range r.context.Imports {
		appendPkg(item.Package, "import_package")
	}
	return result
}

func (r *Resolver) newResolution(expression, target, key, matchKind string) *Resolution {
	if key == "" {
		return nil
	}
	resolution := &Resolution{
		Expression:  strings.TrimSpace(expression),
		Target:      strings.TrimSpace(target),
		ResolvedKey: key,
		MatchKind:   matchKind,
		Provenance:  r.lookupProvenance(key),
	}
	return resolution
}

func (r *Resolver) lookupProvenance(key string) Provenance {
	prov := Provenance{
		Package: packageOf(key),
		Kind:    "registry",
	}
	if built, ok := r.provenance[key]; ok {
		if built.Package != "" {
			prov.Package = built.Package
		}
		if built.File != "" {
			prov.File = built.File
		}
		if built.Kind != "" {
			prov.Kind = built.Kind
		}
	}
	return prov
}

func cloneProvenance(input map[string]Provenance) map[string]Provenance {
	if len(input) == 0 {
		return nil
	}
	result := make(map[string]Provenance, len(input))
	for k, v := range input {
		result[k] = v
	}
	return result
}

func packageOf(key string) string {
	index := strings.LastIndex(key, ".")
	if index == -1 {
		return ""
	}
	return key[:index]
}

func normalizeContext(input *Context) *Context {
	if input == nil {
		return nil
	}
	ret := &Context{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
	}
	for _, item := range input.Imports {
		pkg := strings.TrimSpace(item.Package)
		if pkg == "" {
			continue
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(pkg)
		}
		ret.Imports = append(ret.Imports, Import{
			Alias:   alias,
			Package: pkg,
		})
	}
	if ret.DefaultPackage == "" && len(ret.Imports) == 0 {
		return nil
	}
	return ret
}

func splitQualified(value string) (prefix string, name string, alias bool, qualified bool) {
	index := strings.LastIndex(value, ".")
	if index == -1 {
		return "", value, false, false
	}
	prefix = strings.TrimSpace(value[:index])
	name = strings.TrimSpace(value[index+1:])
	if prefix == "" || name == "" {
		return "", "", false, false
	}
	qualified = true
	alias = !strings.Contains(prefix, "/")
	return prefix, name, alias, qualified
}

func normalizeLookupKey(typeExpr string) string {
	value := strings.TrimSpace(typeExpr)
	for {
		switch {
		case strings.HasPrefix(value, "*"):
			value = strings.TrimPrefix(value, "*")
		case strings.HasPrefix(value, "[]"):
			value = strings.TrimPrefix(value, "[]")
		default:
			return strings.TrimSpace(value)
		}
	}
}
