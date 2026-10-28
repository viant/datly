package transcribe

import (
	"bytes"
	"context"
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	authoring "github.com/viant/datly/transcribe/compile"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/bodygraph"
	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
)

type factoryOwnerPhysical struct {
	connector string
	view      *spec.View
	facts     column.PhysicalSourceConstraints
}

// factoryOwnerReference exists for one compilation and is never serialized. It
// grants neither generated-file ownership nor runtime reader/DML capabilities.
type factoryOwnerReference struct {
	declaration spec.BorrowedSQLRow
	source      *Source
	directory   string
	files       []gen.BorrowedAuthorityFile
	physical    []factoryOwnerPhysical
	result      *Result
	target      *spec.View
	slot        *spec.View
}

func (c *Compiler) refineFactoryOwnerColumns(ctx context.Context, component *spec.Component, source *Source, declarations gen.Declarations, bindings gen.ViewBindings, resolver *typecatalog.Resolver, composition *authoring.FactoryComposition, inputs ...gen.Input) error {
	var proof func(*spec.Component) error
	if c.factoryOwner != nil {
		proof = func(body *spec.Component) error { return c.factoryOwner.discover(ctx, body) }
	} else if len(c.factoryReferences) != 0 {
		proof = func(body *spec.Component) error {
			for _, reference := range c.factoryReferences {
				if err := reference.validate(ctx); err != nil {
					return err
				}
				if err := reference.install(inputs); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return refineComponentColumnsWithFactoryProof(ctx, component, source, declarations, bindings, resolver, proof, composition, inputs...)
}

func (r *factoryOwnerReference) discover(ctx context.Context, body *spec.Component) error {
	target, _, err := bodygraph.ResolveOccurrence(body, r.declaration.OwnerBodyPath)
	if err != nil {
		return err
	}
	visiting := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visiting[view] {
			return fmt.Errorf("factory owner body has incomplete or cyclic relations")
		}
		visiting[view] = true
		defer delete(visiting, view)
		if view.Source != nil && view.Source.Table != "" {
			connector := borrowedConnector(body, view)
			facts, err := r.source.ColumnRefiner.PhysicalTableConstraints(ctx, connector, view.Source.Table)
			if err != nil {
				return err
			}
			r.physical = append(r.physical, factoryOwnerPhysical{connector: connector, view: view, facts: facts})

		}
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil {
				return fmt.Errorf("factory owner relation is incomplete")
			}
			if err := visit(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(target)
}

func factoryPackageCatalog(source *Source, workspace *xmodule.Workspace) (*typecatalog.Catalog, error) {
	if source.Types == nil || source.dependencySnapshot == nil {
		return nil, fmt.Errorf("factory owner requires consumed package source authority")
	}
	catalog := typecatalog.NewCatalog()
	paths := make([]string, 0, len(source.dependencySnapshot.Packages))
	for path := range source.dependencySnapshot.Packages {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		pkg := source.dependencySnapshot.Packages[path]
		location, err := workspace.Package(path)
		if err != nil || location == nil {
			return nil, fmt.Errorf("factory snapshot package location %s: %v", path, err)
		}
		generated := map[string]bool{}
		for _, file := range pkg.Files {
			for _, sealed := range source.dependencySnapshot.Files {
				if filepath.Base(sealed.Path) == filepath.Base(file.Name) && filepath.Dir(sealed.Path) == location.Dir && astGeneratedSource(string(sealed.Bytes)) {
					generated[filepath.Base(file.Name)] = true
				}
			}
		}
		if err := catalog.RegisterPackageFiles(pkg, generated); err != nil {
			return nil, fmt.Errorf("reuse factory source package %s: %w", path, err)
		}
	}
	registry, err := source.Types.Registry(typecatalog.PackageAuthority)
	if err != nil {
		return nil, err
	}
	for _, key := range registry.Keys() {
		origin, found, err := source.Types.Origin(typecatalog.PackageAuthority, key)
		if err != nil {
			return nil, err
		}
		if !found || origin != typecatalog.TypeOriginPackage {
			continue
		}
		descriptor, found, err := source.Types.Resolve(typecatalog.PackageAuthority, key)
		if err != nil {
			return nil, err
		}
		if !found || descriptor == nil || descriptor.Key() != key || source.dependencySnapshot.Packages[descriptor.PkgPath] != nil {
			continue
		}
		if err := catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptor); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *Compiler) prepareFactoryOwnerReferences(ctx context.Context, source *Source, body *spec.Component, resolver *typecatalog.Resolver, linked gen.ViewReferences) error {
	declarations := authoredBorrowedSQLRows(body)
	if len(declarations) == 0 {
		return nil
	}
	if c.factoryOwner != nil {
		return fmt.Errorf("factory linked owners cannot themselves borrow owner bodies")
	}
	if source.dependencySnapshot == nil {
		return fmt.Errorf("factory linked owners require the consumed build-selected source snapshot")
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: source.GoBuild.Dir}).Resolve(ctx)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, declaration := range declarations {
		if err := declaration.Validate(); err != nil {
			return err
		}
		if seen[declaration.BodyPath] {
			return fmt.Errorf("duplicate factory owner occurrence %s", declaration.BodyPath)
		}
		seen[declaration.BodyPath] = true
		slot, _, err := bodygraph.ResolveOccurrence(body, declaration.BodyPath)
		if err != nil {
			return err
		}
		// The Graph occurrence retains independent physical source authority. A
		// containment member alone must never acquire its expected table from owner.
		if slot.Source == nil || slot.Source.Table == "" || len(slot.Columns) != 0 || len(slot.Relations) != 0 || slot.Dest != "" || slot.Source.URI != "" || len(slot.Source.Embeds) != 0 || slot.Source.SQL != "" || slot.Source.Controls != nil {
			return fmt.Errorf("factory owner %s requires a bare independently authored physical occurrence without local projection or relation overrides", declaration.BodyPath)
		}
		canonical, err := (&gen.Plan{Imports: body.TypeContext.Imports}).CanonicalType(body.TypeContext.PackagePath, slot.TypeName)
		if err != nil {
			return err
		}
		if canonical != declaration.Package+"."+declaration.Type {
			return fmt.Errorf("factory owner occurrence nominal type differs from declaration")
		}
		// The selected source descriptor, not a generated substitute, owns this row.
		descriptor, err := resolver.Descriptor(canonical)
		if err != nil || descriptor == nil || descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil {
			return fmt.Errorf("factory owner %s lacks current nominal source authority: %v", canonical, err)
		}
		location, err := workspace.Package(declaration.Package)
		if err != nil || location == nil {
			return fmt.Errorf("factory owner package is outside current workspace: %v", err)
		}
		ownerPath := filepath.Join(location.Module.Dir, filepath.FromSlash(declaration.OwnerURI))
		resolved, err := filepath.EvalSymlinks(ownerPath)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(location.Module.Dir, resolved)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("factory owner resource escapes its source module")
		}
		if resolved != filepath.Clean(ownerPath) || filepath.Clean(ownerPath) == filepath.Clean(source.Path) || strings.TrimSuffix(filepath.Base(ownerPath), filepath.Ext(ownerPath)) != declaration.OwnerName {
			return fmt.Errorf("factory owner resource/source identity differs")
		}
		files, err := sealBorrowedSourcePackage(filepath.Dir(ownerPath))
		if err != nil {
			return err
		}
		var text []byte
		for _, file := range files {
			if filepath.Clean(file.Path) == filepath.Clean(ownerPath) {
				text = file.Bytes
			}
		}
		if text == nil {
			return fmt.Errorf("factory owner DQL resource is missing")
		}
		ownerSource := *source
		ownerSource.Name = declaration.OwnerName
		ownerSource.Path = ownerPath
		ownerSource.Text = string(text)
		ownerSource.Scope = filepath.ToSlash(filepath.Dir(declaration.OwnerURI))
		ownerSource.PackageComponent = nil
		ownerSource.LinkedInputType = nil
		ownerSource.LinkedOutputType = nil
		ownerSource.LinkedHandler = nil
		ownerSource.GoHandler = nil
		ownerSource.VeltyHandler = nil
		ownerSource.Types, err = factoryPackageCatalog(source, workspace)
		if err != nil {
			return err
		}
		ownerSource.Resources, err = capturedBorrowedResources(&ownerSource, files)
		if err != nil {
			return err
		}
		files = append(files, source.dependencySnapshot.Files...)
		ref := &factoryOwnerReference{declaration: declaration, source: &ownerSource, directory: location.Dir, files: files}
		ownerCompiler := &Compiler{factoryOwner: ref}
		owner, err := ownerCompiler.Compile(ctx, &ownerSource)
		if err != nil {
			return fmt.Errorf("factory owner %s: %w", declaration.OwnerURI, err)
		}
		if owner.Component == nil || owner.Component.TypeContext == nil || owner.Component.TypeContext.PackagePath != declaration.Package {
			return fmt.Errorf("factory owner canonical package differs")
		}
		ownerBody := owner.Component.Clone()
		nativeOwner := owner.ExternalHandler == nil && ownerBody.RootView != nil
		if nativeOwner {
			// Use the same declared holder pairing as ordinary native borrowing.
			pairingWorkspace, pairErr := (xmodule.LocalWorkspace{BaseDir: location.Dir}).Resolve(ctx)
			if pairErr != nil {
				return pairErr
			}
			pairing := &discoveryCompilation{workspace: pairingWorkspace, catalog: ownerSource.Types, discovery: &Discovery{BaseDir: location.Dir, GoBuild: source.GoBuild}}
			if _, pairErr = pairing.declaredPackageSource(ctx, &ownerSource); pairErr != nil {
				return pairErr
			}
			owner, err = ownerCompiler.Compile(ctx, &ownerSource)
			if err != nil {
				return err
			}
			ownerBody = owner.Component.Clone()
			if err = ref.discover(ctx, ownerBody); err != nil {
				return err
			}
		} else if owner.ExternalHandler != nil && owner.ExternalHandler.InputShape != nil && ownerBody.RootView == nil {
			ownerBody.RootView = owner.ExternalHandler.InputShape
		} else {
			return fmt.Errorf("factory owner is not a native writer or source-factory body")
		}
		target, _, err := bodygraph.ResolveOccurrence(ownerBody, declaration.OwnerBodyPath)
		if err != nil {
			return err
		}
		wanted, err := (&gen.Plan{Imports: owner.Component.TypeContext.Imports}).CanonicalType(declaration.Package, target.TypeName)
		if err != nil || wanted != canonical {
			return fmt.Errorf("factory owner body occurrence nominal identity differs: %v", err)
		}
		if target.Source == nil || target.Source.Table != slot.Source.Table || borrowedConnector(ownerBody, target) != borrowedConnector(body, slot) {
			return fmt.Errorf("factory owner physical table/connector differs from independently authored occurrence")
		}
		// Reuse complete native owner authority. The ownership-sensitive leaf API is
		// deliberately not called, and relations are never removed to satisfy it.
		var artifacts []gen.BorrowedAuthorityFile
		if nativeOwner {
			ownerInput, inputErr := borrowedGenerationInput(ctx, owner, declaration.Package)
			if inputErr != nil {
				return inputErr
			}
			plan, planErr := gen.BorrowedOwnerPlan(ownerInput)
			if planErr != nil {
				return planErr
			}
			entry := &borrowedOwnerPreparation{source: ownerSource, result: owner, input: ownerInput, plan: plan}
			if _, err = prepareBorrowedNativeOwner(entry, &ownerSource, owner, target, location.Dir, declaration.Package); err != nil {
				return err
			}
			artifacts = entry.selection.Artifacts
		} else {
			ownerInput, ownerDirectory, inputErr := generationInput(location.Module.Dir, "", owner)
			if inputErr != nil {
				return fmt.Errorf("factory owner generation identity: %w", inputErr)
			}
			if filepath.Clean(filepath.Join(location.Module.Dir, ownerDirectory)) != filepath.Clean(location.Dir) {
				return fmt.Errorf("factory owner generated destination differs from nominal package")
			}
			artifacts, err = gen.BorrowedNativeOwnerArtifacts(ownerInput, location.Dir)
			if err != nil {
				return fmt.Errorf("factory owner original native artifacts: %w", err)
			}
		}
		ref.files = append(ref.files, artifacts...)
		ref.result = owner
		ref.target = target
		ref.slot = slot
		if err := ref.validate(ctx); err != nil {
			return err
		}
		c.factoryReferences = append(c.factoryReferences, ref)
	}
	return nil
}

func (r *factoryOwnerReference) validate(ctx context.Context, mappings ...func(string) string) error {
	for _, sealed := range r.files {
		paths := []string{sealed.Path}
		if len(mappings) > 0 && mappings[0] != nil {
			mapped := mappings[0](sealed.Path)
			if mapped != sealed.Path {
				paths = append(paths, mapped)
			}
		}
		for _, path := range paths {
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Mode() != sealed.Mode {
				return fmt.Errorf("factory owner dependency mode/type drift: %s", sealed.Path)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Equal(data, sealed.Bytes) {
				return fmt.Errorf("factory owner dependency content drift: %s", sealed.Path)
			}
		}
	}
	for _, physical := range r.physical {
		fresh, err := r.source.ColumnRefiner.PhysicalTableConstraints(ctx, physical.connector, physical.facts.Identity.Table)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(fresh, physical.facts) {
			return fmt.Errorf("factory owner native schema changed after source compilation")
		}
	}
	return nil
}

func (c *Compiler) validateFactoryOwnerSources(ctx context.Context) func(func(string) string) error {
	if len(c.factoryReferences) == 0 {
		return nil
	}
	references := append([]*factoryOwnerReference(nil), c.factoryReferences...)
	return func(mapPath func(string) string) error {
		for _, reference := range references {
			if err := reference.validate(ctx, mapPath); err != nil {
				return err
			}
		}
		return nil
	}
}

// factoryPhysicalCompositionJoin binds containment to an existing authored tuple
// and a separately parsed physical source. It does not admit the owner; the full
// source/nominal/schema proof in prepareFactoryOwnerReferences remains mandatory.
func factoryPhysicalCompositionJoin(component *spec.Component, bodyPath, table, alias, parentAlias, holder, canonicalMember string, cardinality spec.Cardinality) (*authoring.FactoryCompositionJoin, error) {
	var selected *spec.BorrowedSQLRow
	for _, declaration := range authoredBorrowedSQLRows(component) {
		if declaration.BodyPath == bodyPath {
			if selected != nil {
				return nil, fmt.Errorf("duplicate factory owner member path %s", bodyPath)
			}
			copy := declaration
			selected = &copy
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("physical composition member %s requires exact borrow_sql_row authority", bodyPath)
	}
	if err := selected.Validate(); err != nil {
		return nil, err
	}
	expected := selected.Package + "." + selected.Type
	wrapper := "[]*"
	if cardinality == spec.CardinalityOne {
		wrapper = "*"
	}
	if canonicalMember != wrapper+expected {
		return nil, fmt.Errorf("physical composition member %s nominal/container identity differs", bodyPath)
	}
	if !token.IsIdentifier(table) || table == "_" || alias == "" || parentAlias == "" || holder == "" {
		return nil, fmt.Errorf("physical composition requires an exact independently parsed table/member source")
	}
	return &authoring.FactoryCompositionJoin{Source: table, Alias: alias, ParentAlias: parentAlias, Holder: holder, TypeName: expected, Cardinality: cardinality, Logical: false}, nil
}

// install publishes only a detached authoring shape after both independent
// physical occurrences have been discovered. The original owner result keeps
// its destinations, route, resources and executable metadata; none become Graph
// runtime capabilities or generated-file ownership.
func (r *factoryOwnerReference) install(inputs []gen.Input) error {
	if r.target == nil || r.slot == nil || len(inputs) != 1 || inputs[0].Views == nil {
		return fmt.Errorf("factory owner installation lacks exact occurrence authority")
	}
	copy := r.target.Clone()
	authority := &gen.Plan{Imports: r.result.Component.TypeContext.Imports}
	var qualify func(*spec.View) error
	qualify = func(view *spec.View) error {
		if view == nil {
			return fmt.Errorf("factory owner contains nil view")
		}
		// Materialize the detached projection with its owner's captured resources.
		// Relative embed paths belong to the owner, not the composing factory.
		if err := dsql.ResolveSource(view.Name, view.Source, r.source.Const.Resources(r.source.Resources)); err != nil {
			return err
		}
		view.Key.Scope = r.slot.Key.Scope + "/owner/" + r.declaration.BodyPath
		// Reference-only ownership uses the actual nominal source; destinations stay
		// exclusively on the unchanged native owner result.
		view.Dest = ""
		// Only this detached input projection loses execution controls. The
		// canonical owner's graph/artifacts and lifecycle remain unchanged.
		view.EntityHooks = ""
		view.QueueContract = ""
		view.WriterActionPolicy = ""
		view.Reconciliation = nil
		view.MutationPredicateGroup = nil
		if view.TypeName != "" {
			canonical, err := authority.CanonicalType(r.declaration.Package, view.TypeName)
			if err != nil {
				return err
			}
			view.TypeName = canonical
		}
		for _, column := range view.Columns {
			if column == nil {
				return fmt.Errorf("factory owner contains nil column")
			}
			if column.Type.Name != "" && column.Type.Package == "" {
				canonical, err := authority.CanonicalType(r.declaration.Package, column.Type.Name)
				if err != nil {
					return err
				}
				column.Type.Name = canonical
			}
		}
		for _, relation := range view.Relations {
			if relation == nil {
				return fmt.Errorf("factory owner contains nil relation")
			}
			if err := qualify(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	if err := qualify(copy); err != nil {
		return err
	}
	// Preserve the Graph holder's independently proven identity and container.
	copy.Key, copy.Name, copy.Namespace, copy.Cardinality = r.slot.Key, r.slot.Name, r.slot.Namespace, r.slot.Cardinality
	oldID, err := r.slot.Identity()
	if err != nil {
		return err
	}
	*r.slot = *copy
	inputs[0].Views[oldID] = &gen.ViewReference{DescriptorKey: r.declaration.Package + "." + r.declaration.Type}
	return nil
}
