package typecatalog

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type AmbiguityError struct {
	Expression string
	Candidates []string
}

func (e *AmbiguityError) Error() string {
	return fmt.Sprintf("ambiguous type %q: candidates=%s", e.Expression, strings.Join(e.Candidates, ","))
}

// Resolver applies package/import context over an immutable catalog snapshot.
// Native shape resolution and public results receive detached descriptors.
type Resolver struct {
	authority    Authority
	types        map[string]*x.Type
	context      *ResolutionContext
	provenance   map[string]Provenance
	implicitTime bool
}

func NewResolver(catalog *Catalog, authority Authority, context *ResolutionContext) (*Resolver, error) {
	return NewResolverWithProvenance(catalog, authority, context, nil)
}

func NewResolverWithProvenance(catalog *Catalog, authority Authority, context *ResolutionContext, provenance map[string]Provenance) (*Resolver, error) {
	types, err := catalog.resolverTypes(authority)
	if err != nil {
		return nil, err
	}
	// SQLX discovers timestamps as the standard library's time.Time. Retain
	// that native package authority when no caller descriptor was supplied;
	// synthetic reconstruction cannot represent its private implementation.
	implicitTime := types["time.Time"] == nil
	if implicitTime {
		types["time.Time"] = x.NewType(reflect.TypeOf(time.Time{}))
	}
	return &Resolver{authority: authority, types: types, context: NormalizeContext(context), provenance: cloneProvenance(provenance), implicitTime: implicitTime}, nil
}

func (r *Resolver) Resolve(typeExpr string) (string, error) {
	resolved, err := r.ResolveWithProvenance(typeExpr)
	if err != nil || resolved == nil {
		return "", err
	}
	return resolved.ResolvedKey, nil
}

// Type resolves an expression to the catalog's effective reflect type.
func (r *Resolver) Type(typeExpr string) (reflect.Type, error) {
	if reference, err := (xshape.Resolver{}).Reference(typeExpr); err == nil && len(reference.Arguments) > 0 {
		typ, err := r.Descriptor(typeExpr)
		if err != nil || typ == nil {
			return nil, err
		}
		return typ.Type, nil
	}
	resolved, err := r.ResolveWithProvenance(typeExpr)
	if err != nil || resolved == nil {
		return nil, err
	}
	// Runtime identity is immutable; no synthetic AST needs to escape.
	typ := r.types[resolved.ResolvedKey]
	if typ == nil {
		return nil, nil
	}
	return typ.Type, nil
}

// Descriptor resolves an expression to its detached viant/x type descriptor.
// Synthetic-only generated types are returned without fabricating reflect.Type.
func (r *Resolver) Descriptor(typeExpr string) (*x.Type, error) {
	if reference, err := (xshape.Resolver{}).Reference(typeExpr); err == nil && len(reference.Arguments) > 0 {
		resolved, err := r.ResolveShape(typeExpr)
		if err != nil || resolved == nil {
			return nil, err
		}
		return resolved.Descriptor, nil
	}
	resolved, err := r.ResolveWithProvenance(typeExpr)
	if err != nil || resolved == nil {
		return nil, err
	}
	typ := r.types[resolved.ResolvedKey]
	if typ == nil {
		return nil, nil
	}
	return CloneDescriptor(typ)
}

// ResolveShape resolves the complete expression identity and its base
// descriptor under this resolver's package and import authority.
func (r *Resolver) ResolveShape(typeExpr string) (*xshape.Resolution, error) {
	if r == nil || r.types == nil {
		return nil, nil
	}
	resolved, err := r.shapeResolver().Resolve(typeExpr)
	if err != nil {
		return nil, fmt.Errorf("resolve type shape %q: %w", typeExpr, err)
	}
	// Both lookup paths detach descriptors before native shape resolution.
	return resolved, nil
}

func (r *Resolver) ResolveWithProvenance(typeExpr string) (*Resolution, error) {
	if r == nil || r.types == nil {
		return nil, nil
	}
	expressions := xshape.Resolver{}
	base, err := expressions.Named(typeExpr)
	if err != nil {
		return nil, fmt.Errorf("resolve type %q: %w", typeExpr, err)
	}
	if base == "" {
		return nil, nil
	}
	candidate, err := r.resolveNamedCandidate(typeExpr, base)
	if err != nil {
		return nil, err
	}
	reference, referenceErr := expressions.Reference(base)
	if referenceErr == nil && reference.BaseName != reference.Name {
		resolved, resolveErr := r.shapeResolver().Resolve(typeExpr)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve type %q: %w", typeExpr, resolveErr)
		}
		if resolved != nil && resolved.Descriptor != nil {
			matchKind := "canonical_generic"
			if candidate != nil && candidate.key == resolved.Descriptor.Key() {
				matchKind = candidate.matchKind
			}
			return r.newResolution(typeExpr, "", resolved.Descriptor.Key(), matchKind), nil
		}
	}
	if candidate == nil {
		return nil, nil
	}
	return r.newResolution(typeExpr, "", candidate.key, candidate.matchKind), nil
}

func (r *Resolver) shapeResolver() xshape.Resolver {
	result := xshape.Resolver{Lookup: r.lookupCanonical, Imports: map[string]string{}}
	if r == nil || r.context == nil {
		return result
	}
	result.Package = strings.TrimSpace(r.context.PackagePath)
	if result.Package == "" {
		result.Package = strings.TrimSpace(r.context.DefaultPackage)
	}
	for _, item := range r.context.Imports {
		result.Imports[strings.TrimSpace(item.Alias)] = strings.TrimSpace(item.Package)
	}
	if name := strings.TrimSpace(r.context.PackageName); name != "" && result.Package != "" {
		result.Imports[name] = result.Package
	}
	return result
}

// CanonicalDeclaration applies authored import/default-package authority to a
// type declaration in a destination-normalized defaultPackage. Source lookup
// scope is not a declaration destination. Native shape owns expression syntax.
func (r *Resolver) CanonicalDeclaration(expression, defaultPackage string) (string, error) {
	resolver := r.shapeResolver()
	resolver.Package = defaultPackage
	return resolver.Canonical(expression)
}

// Native shape resolution calls Lookup with canonical names first. Those
// names must not be interpreted a second time as authored import aliases.
func (r *Resolver) lookupCanonical(expression string) (*x.Type, error) {
	name, err := (xshape.Resolver{}).Named(expression)
	if err != nil {
		return nil, err
	}
	if descriptor := r.types[name]; descriptor != nil {
		return CloneDescriptor(descriptor)
	}
	return r.lookupStructural(expression)
}

func (r *Resolver) lookupStructural(typeExpr string) (*x.Type, error) {
	base, err := (xshape.Resolver{}).Named(typeExpr)
	if err != nil {
		return nil, err
	}
	candidate, err := r.resolveNamedCandidate(typeExpr, base)
	if err != nil || candidate == nil {
		return nil, err
	}
	return CloneDescriptor(r.types[candidate.key])
}

func (r *Resolver) resolveNamedCandidate(expression, base string) (*candidate, error) {
	// An authored import qualifier is source authority, including a qualifier
	// that happens to spell a standard-library package name.
	if reference, err := (xshape.Resolver{}).Reference(base); err == nil && r.context != nil && reference.Qualifier != "" {
		for _, imported := range r.context.Imports {
			if imported.Alias != reference.Qualifier {
				continue
			}
			for _, name := range referenceNames(reference) {
				key := imported.Package + "." + name
				if r.types[key] != nil {
					return &candidate{key: key, matchKind: "alias_import"}, nil
				}
			}
			return nil, nil
		}
	}
	if r.types[base] != nil {
		return &candidate{key: base, matchKind: "exact"}, nil
	}
	if strings.Contains(base, "/") {
		return nil, nil
	}
	reference, err := (xshape.Resolver{}).Reference(base)
	if err != nil {
		return nil, nil
	}
	prefix, baseName := reference.Qualifier, reference.Name
	qualified := prefix != ""
	if qualified {
		if prefix == "" || baseName == "" {
			return nil, nil
		}
		packagePath := r.aliasPackage(prefix)
		if packagePath == "" {
			return nil, nil
		}
		for _, candidateName := range referenceNames(reference) {
			key := packagePath + "." + candidateName
			if r.types[key] != nil {
				return &candidate{key: key, matchKind: "alias_import"}, nil
			}
		}
		return nil, nil
	}
	// Go package declarations shadow same-named types in imported packages.
	// Transcription authority retains its multi-source ambiguity policy.
	if r.authority == PackageAuthority && r.context != nil && r.context.PackagePath != "" {
		for _, name := range referenceNames(reference) {
			key := r.context.PackagePath + "." + name
			if r.types[key] != nil {
				return &candidate{key: key, matchKind: "package_path"}, nil
			}
		}
	}
	candidates := r.unqualifiedCandidates(baseName)
	if len(candidates) == 0 && reference.BaseName != baseName {
		candidates = r.unqualifiedCandidates(reference.BaseName)
	}
	if len(candidates) == 1 {
		return &candidates[0], nil
	}
	if len(candidates) > 1 {
		keys := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			keys = append(keys, candidate.key)
		}
		sort.Strings(keys)
		return nil, &AmbiguityError{Expression: expression, Candidates: keys}
	}
	return nil, nil
}

func referenceNames(reference xshape.Reference) []string {
	if reference.BaseName == "" || reference.BaseName == reference.Name {
		return []string{reference.Name}
	}
	return []string{reference.Name, reference.BaseName}
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
	if r.context.PackageName == alias {
		return r.context.PackagePath
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
		key := scoped.packagePath + "." + typeName
		if seen[key] {
			continue
		}
		seen[key] = true
		if r.types[key] != nil {
			result = append(result, candidate{key: key, matchKind: scoped.matchKind})
		}
	}
	if len(result) > 0 {
		return result
	}
	for key := range r.types {
		if r.implicitTime && key == "time.Time" {
			continue
		}
		if key != typeName && !strings.HasSuffix(key, "."+typeName) || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, candidate{key: key, matchKind: "global_unique"})
	}
	return result
}

type scopedPackage struct {
	packagePath string
	matchKind   string
}

func (r *Resolver) searchPackages() []scopedPackage {
	if r.context == nil {
		return nil
	}
	seen := map[string]bool{}
	var result []scopedPackage
	appendPackage := func(packagePath, matchKind string) {
		packagePath = strings.TrimSpace(packagePath)
		if packagePath == "" || seen[packagePath] {
			return
		}
		seen[packagePath] = true
		result = append(result, scopedPackage{packagePath: packagePath, matchKind: matchKind})
	}
	appendPackage(r.context.PackagePath, "package_path")
	appendPackage(r.context.DefaultPackage, "default_package")
	for _, item := range r.context.Imports {
		appendPackage(item.Package, "import_package")
	}
	return result
}

func (r *Resolver) newResolution(expression, target, key, matchKind string) *Resolution {
	if key == "" {
		return nil
	}
	return &Resolution{
		Expression: strings.TrimSpace(expression), Target: strings.TrimSpace(target),
		ResolvedKey: key, MatchKind: matchKind, Provenance: r.lookupProvenance(key),
	}
}

func (r *Resolver) lookupProvenance(key string) Provenance {
	result := Provenance{Package: packageOf(key), Kind: "registry"}
	if provenance, ok := r.provenance[key]; ok {
		if provenance.Package != "" {
			result.Package = provenance.Package
		}
		if provenance.File != "" {
			result.File = provenance.File
		}
		if provenance.Kind != "" {
			result.Kind = provenance.Kind
		}
	}
	return result
}

func cloneProvenance(input map[string]Provenance) map[string]Provenance {
	if len(input) == 0 {
		return nil
	}
	result := make(map[string]Provenance, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func packageOf(key string) string {
	if packagePath, _, err := (xshape.Resolver{}).CanonicalReference(key); err == nil {
		return packagePath
	}
	if index := strings.LastIndex(key, "."); index != -1 {
		return key[:index]
	}
	return ""
}
