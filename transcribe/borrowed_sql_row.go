package transcribe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/transcribe/internal/bodygraph"
	"github.com/viant/datly/transcribe/internal/dependency"
	"github.com/viant/datly/transcribe/internal/handlergen"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	sqlexpr "github.com/viant/sqlparser/expr"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
)

func authoredBorrowedSQLRows(c *spec.Component) []spec.BorrowedSQLRow {
	if c == nil || c.Settings == nil || c.Settings.Generation == nil {
		return nil
	}
	return append([]spec.BorrowedSQLRow(nil), c.Settings.Generation.BorrowedSQLRows...)
}

func borrowedLeafContract(ctx context.Context, result *Result, view *spec.View, declaration spec.BorrowedSQLRow) (gen.BorrowedLeafContract, error) {
	if result == nil || result.Source == nil || result.Source.ColumnRefiner == nil || view == nil || view.Source == nil {
		return gen.BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row requires fresh physical source discovery")
	}
	// Discovery preserves authored URI/embed provenance. Resolve a detached leaf
	// from the same captured resources before proving its physical SQL ownership.
	physicalView := view.Clone()
	var resources fs.FS
	if result.Source.Resources != nil {
		resources = result.Source.Const.Resources(result.Source.Resources)
	}
	if err := dsql.ResolveSource(view.Name, physicalView.Source, resources); err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	if err := borrowedPhysicalProjection(physicalView); err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	facts, err := result.Source.ColumnRefiner.PhysicalSourceConstraints(ctx, borrowedConnector(result.Component, view), physicalView)
	if err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	origin := facts.Identity
	comparisonView := view
	if view.Auxiliary && view != result.Component.RootView && view.NestedNullPolicy == "skip-auxiliary" {
		// Validate the original collection role before detaching it. A shared row
		// comparison must not manufacture eligibility for an invalid policy.
		if err := (&gen.Input{Component: result.Component}).ValidateLifecycleTarget(true); err != nil {
			return gen.BorrowedLeafContract{}, err
		}
		// Independent row generation promotes this leaf to a root. Its nested
		// auxiliary traversal policy remains on the canonical/runtime occurrence,
		// while the physical owner's shared row and Has contract stays unchanged.
		comparisonView = view.Clone()
		comparisonView.NestedNullPolicy = ""
	}
	comparisonInput, err := borrowedGenerationInput(ctx, result, declaration.Package)
	if err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	var contract gen.BorrowedLeafContract
	if result.borrowedPreparation != nil && result.borrowedPreparation.input != nil {
		plan := result.borrowedPreparation.plan
		if plan == nil {
			plan, err = gen.BorrowedOwnerPlan(comparisonInput)
			if err == nil {
				result.borrowedPreparation.plan = plan
			}
		}
		if err != nil {
			return contract, err
		}
		contract, err = gen.BorrowedLeafContractFromPlan(plan, comparisonView, declaration.Package, declaration.Type, origin.Connector, origin.Catalog, origin.Schema, origin.Table)
	} else {
		contract, err = gen.BorrowedLeafContractFor(result.Component, comparisonView, declaration.Package, declaration.Type, origin.Connector, origin.Catalog, origin.Schema, origin.Table, comparisonInput)
	}

	if err != nil {
		return contract, err
	}
	if err = applyBorrowedPhysicalFacts(&contract, facts); err != nil {
		return contract, err
	}
	path := declaration.BodyPath
	if result.Source.Name == declaration.OwnerName {
		path = declaration.OwnerBodyPath
	}
	if result.borrowedPreparation != nil && result.borrowedPreparation.plan != nil {
		contract.Wrapper, err = gen.BorrowedBodyWrapperFromPlan(result.borrowedPreparation.plan, path, declaration.Package)
	} else {
		contract.Wrapper, err = gen.BorrowedBodyWrapper(result.Component, path, declaration.Package, comparisonInput)
	}
	if err != nil {
		return contract, fmt.Errorf("borrow_sql_row wrapper: %w", err)
	}
	return contract, nil
}

func borrowedConnector(c *spec.Component, v *spec.View) string {
	if v.Source != nil && v.Source.Bindings != nil && v.Source.Bindings.Connector != "" {
		return v.Source.Bindings.Connector
	}
	if c.Settings != nil {
		return c.Settings.DefaultConnector
	}
	return ""
}

// freshBorrowedSource has no body/package overlay or generated row catalog.
// The filesystem resources are captured as immutable bytes before discovery.
func freshBorrowedSource(ctx context.Context, source *Source, text string, files []gen.BorrowedAuthorityFile, dependencies ...*typecatalog.Catalog) (*Result, error) {
	foreign, err := freshBorrowedForeignTypes(ctx, source, text)
	if err != nil {
		return nil, err
	}
	return compileBorrowedSource(ctx, source, text, files, foreign, dependencies...)
}

func compileBorrowedSource(ctx context.Context, source *Source, text string, files []gen.BorrowedAuthorityFile, foreign *typecatalog.Catalog, dependencies ...*typecatalog.Catalog) (*Result, error) {
	copy := *source
	copy.Text = text
	copy.PackageComponent = nil
	copy.LinkedInputType = nil
	copy.LinkedOutputType = nil
	var err error
	copy.Types, err = foreign.Clone()
	if err != nil {
		return nil, err
	}
	if len(dependencies) > 0 && dependencies[0] != nil {
		var err error
		copy.Types, err = mergeBorrowedOwnerDependencies(copy.Types, dependencies[0])
		if err != nil {
			return nil, err
		}
	}
	var captured []packageasset.File
	for _, f := range files {
		rel, err := filepath.Rel(source.BaseDir(), f.Path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
			captured = append(captured, packageasset.File{Path: filepath.ToSlash(rel), Data: append([]byte(nil), f.Bytes...)})
		}
	}
	snapshot, err := (packageasset.Snapshotter{Overlay: captured}).All(ctx)
	if err != nil {
		return nil, err
	}
	copy.Resources, err = resource.New().WithDefault(snapshot)
	if err != nil {
		return nil, err
	}
	result, err := (&Compiler{comparisonOnly: true}).Compile(ctx, &copy)
	if err == nil {
		result.borrowedPreparation = &borrowedPreparation{foreign: foreign}
	}
	return result, err
}

func validateBorrowedSourceText(source *Source, files []gen.BorrowedAuthorityFile) error {
	for _, file := range files {
		if filepath.Clean(file.Path) == filepath.Clean(source.Path) {
			if string(file.Bytes) != source.Text {
				return fmt.Errorf("borrow_sql_row consumed DQL differs from captured source bytes")
			}
			return nil
		}
	}
	return fmt.Errorf("borrow_sql_row persistent authored source is missing")
}
func validateBorrowedConsumedFiles(consumed, current []gen.BorrowedAuthorityFile) error {
	if len(consumed) == 0 {
		return fmt.Errorf("borrow_sql_row consumed source/resource evidence is missing")
	}
	if len(consumed) != len(current) {
		return fmt.Errorf("borrow_sql_row source/resource membership changed before sealing")
	}
	lookup := map[string]gen.BorrowedAuthorityFile{}
	for _, file := range current {
		lookup[file.Path] = file
	}
	for _, file := range consumed {
		other, ok := lookup[file.Path]
		if !ok || file.Mode != other.Mode || !bytes.Equal(file.Bytes, other.Bytes) {
			return fmt.Errorf("borrow_sql_row consumed source/resource changed before sealing: %s", file.Path)
		}
	}
	return nil
}
func capturedBorrowedResources(source *Source, files []gen.BorrowedAuthorityFile) (*resource.Store, error) {
	var captured []packageasset.File
	for _, file := range files {
		rel, err := filepath.Rel(source.BaseDir(), file.Path)
		if err != nil {
			return nil, err
		}
		captured = append(captured, packageasset.File{Path: filepath.ToSlash(rel), Data: append([]byte(nil), file.Bytes...)})
	}
	snapshot, err := (packageasset.Snapshotter{Overlay: captured}).All(context.Background())
	if err != nil {
		return nil, err
	}
	resources := source.Resources
	if resources == nil {
		resources = resource.New()
	}
	return resources.WithDefault(snapshot)
}

func sealBorrowedSourcePackage(base string) ([]gen.BorrowedAuthorityFile, error) {
	var files []gen.BorrowedAuthorityFile
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".dql" && extension != ".sql" {
			return nil
		}
		f, err := gen.SealBorrowedAuthorityFile(path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	return files, err
}

func (c *Compiler) admitBorrowedSQLRows(ctx context.Context, result *Result) error {
	if c.comparisonOnly {
		return nil
	}
	declarations := result.AuthoredBorrowedSQLRows
	if len(declarations) == 0 {
		if len(authoredBorrowedSQLRows(result.Component)) != 0 {
			return fmt.Errorf("borrow_sql_row inherited settings cannot establish borrowing intent")
		}
		return nil
	}
	source := result.Source
	if source.ColumnRefiner == nil || source.BaseDir() == "" {
		return fmt.Errorf("borrow_sql_row requires current SQL schema discovery and an exact source resource directory")
	}
	if result.Component.TypeContext == nil {
		return fmt.Errorf("borrow_sql_row canonical package authority is missing")
	}
	files, err := sealBorrowedSourcePackage(source.BaseDir())
	if err != nil {
		return err
	}
	if err = validateBorrowedConsumedFiles(result.borrowedConsumedFiles, files); err != nil {
		return err
	}
	if err = validateBorrowedSourceText(source, files); err != nil {
		return err
	}
	preparation := result.borrowedPreparation
	if preparation == nil {
		return fmt.Errorf("borrow_sql_row owner preparation is missing")
	}
	freshDependencies := preparation.dependencies
	admittedDependencies, err := borrowedNamedContracts(freshDependencies)
	if err != nil {
		return err
	}
	freshBorrower, err := freshBorrowedSource(ctx, source, source.Text, files, freshDependencies)
	if err != nil {
		return err
	}
	if len(authoredBorrowedSQLRows(freshBorrower.Component)) != len(declarations) {
		return fmt.Errorf("borrow_sql_row fresh authoring declarations differ")
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: source.BaseDir()}).Resolve(ctx)
	if err != nil {
		return err
	}
	for _, declaration := range declarations {
		if err := declaration.Validate(); err != nil {
			return err
		}
		if declaration.Package != result.Component.TypeContext.PackagePath {
			return fmt.Errorf("borrow_sql_row cross-package generated borrowing is unsupported")
		}
		ownerPath := filepath.Join(source.BaseDir(), filepath.FromSlash(declaration.OwnerURI))
		if filepath.Clean(ownerPath) == filepath.Clean(source.Path) || strings.TrimSuffix(filepath.Base(ownerPath), filepath.Ext(ownerPath)) != declaration.OwnerName {
			return fmt.Errorf("borrow_sql_row owner must identify a different exact Source.Name and resource")
		}
		entry := preparation.owners[borrowedOwnerKey(source.BaseDir(), declaration)]
		if entry == nil {
			return fmt.Errorf("borrow_sql_row exact owner preparation is missing")
		}
		ownerSource := entry.source
		freshOwner := entry.result
		if len(authoredBorrowedSQLRows(freshOwner.Component)) != 0 {
			return fmt.Errorf("borrow_sql_row borrowed owners/cyclic dependencies are unsupported")
		}
		if freshOwner.Component.TypeContext == nil || freshOwner.Component.TypeContext.PackagePath != declaration.Package {
			return fmt.Errorf("borrow_sql_row owner canonical package differs")
		}
		owner, ownerGraph, err := bodygraph.ResolveBorrowed(freshOwner.Component, declaration.OwnerBodyPath)
		if err != nil {
			return err
		}
		if owner.Auxiliary {
			if owner != freshOwner.Component.RootView {
				return fmt.Errorf("borrow_sql_row auxiliary owner requires its canonical lifecycle body leaf")
			}
			if err := (&gen.Input{Component: freshOwner.Component}).ValidateLifecycleTarget(true); err != nil {
				return err
			}
		}
		borrower, borrowerGraph, err := bodygraph.ResolveBorrowed(freshBorrower.Component, declaration.BodyPath)
		if err != nil {
			return err
		}
		actualSlot, actualGraph, err := bodygraph.ResolveBorrowed(result.Component, declaration.BodyPath)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actualGraph, borrowerGraph) {
			return fmt.Errorf("borrow_sql_row package overlay changed the canonical body slot")
		}
		if actualSlot.Dest != "" || borrower.Dest != "" {
			return fmt.Errorf("borrow_sql_row borrowed slot cannot declare dest")
		}
		canonicalRow := declaration.Package + "." + declaration.Type
		for _, occurrence := range []struct {
			result   *Result
			view     *spec.View
			required bool
		}{{freshOwner, owner, true}, {freshBorrower, borrower, false}, {result, actualSlot, false}} {
			if occurrence.view.TypeName == "" && !occurrence.required {
				continue
			}
			canonical, err := occurrence.result.TypeResolver.CanonicalDeclaration(occurrence.view.TypeName, declaration.Package)
			if err != nil {
				return err
			}
			if canonical != canonicalRow {
				return fmt.Errorf("borrow_sql_row explicit canonical row type differs")
			}
		}
		if err := column.ApplyWriterMetadata(freshOwner.Component); err != nil {
			return err
		}
		if err := column.ApplyWriterMetadata(freshBorrower.Component); err != nil {
			return err
		}
		expected, err := borrowedLeafContract(ctx, freshOwner, owner, declaration)
		if err != nil {
			return err
		}
		wanted, err := borrowedLeafContract(ctx, freshBorrower, borrower, declaration)
		if err != nil {
			return err
		}
		if err := gen.CompareBorrowedLeafContracts(expected, wanted); err != nil {
			return err
		}
		pkg, err := dependency.LoadPackage(ctx, workspace, declaration.Package)
		if err != nil {
			return fmt.Errorf("borrow_sql_row owner descriptor unavailable; generate owner first: %w", err)
		}
		if pkg == nil {
			return fmt.Errorf("borrow_sql_row owner descriptor is missing; generate owner first")
		}
		if source.Types == nil {
			source.Types = typecatalog.NewCatalog()
		}
		if err := source.Types.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			return err
		}
		resolver, err := typecatalog.NewResolver(source.Types, typecatalog.TranscribeAuthority, result.TypeContext)
		if err != nil {
			return err
		}
		row, err := resolver.Descriptor(declaration.Package + "." + declaration.Type)
		if err != nil {
			return err
		}
		marker, err := resolver.Descriptor(declaration.Package + "." + declaration.Type + "Has")
		if err != nil {
			return err
		}
		if err := gen.ValidateBorrowedDescriptor(expected, row, marker); err != nil {
			return err
		}
		location, err := workspace.Package(declaration.Package)
		if err != nil {
			return err
		}
		proof := &gen.BorrowedRowAuthority{Declaration: declaration, BorrowerSource: source.Path, BorrowerScope: source.Scope, BorrowerName: source.Name, BorrowerGraph: borrowerGraph, OwnerSource: ownerPath, OwnerScope: ownerSource.Scope, OwnerName: ownerSource.Name, OwnerGraph: ownerGraph, OwnerDirectory: location.Dir, Expected: expected, Files: append([]gen.BorrowedAuthorityFile(nil), files...)}
		var holderFiles []xmodule.File
		ownerEntries, err := os.ReadDir(location.Dir)
		if err != nil {
			return err
		}
		for _, entry := range ownerEntries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
				holderFiles = append(holderFiles, xmodule.File{Path: filepath.Join(location.Dir, entry.Name()), Dir: location.Dir, ImportPath: declaration.Package})
			}
		}
		routes, err := (bootstrap.PackageDiscovery{}).DiscoverFiles(holderFiles)
		if err != nil {
			return err
		}
		matches := 0
		ownerMutation := ""
		matchedRoutes := map[int]bool{}
		for _, holder := range routes {
			if holder.Tag.Name != ownerSource.Name {
				continue
			}
			index := -1
			for i, route := range freshOwner.Component.Routes {
				if route != nil && route.Path == holder.Tag.Path && strings.EqualFold(route.Method, holder.Tag.Method) {
					if index >= 0 {
						return fmt.Errorf("borrow_sql_row owner route identity is ambiguous")
					}
					index = i
				}
			}
			if index < 0 || matchedRoutes[index] || holder.PackagePath != declaration.Package || holder.Tag.Connector != borrowedConnector(freshOwner.Component, owner) {
				return fmt.Errorf("borrow_sql_row actual owner holder differs from exact fresh source identity")
			}
			route := freshOwner.Component.Routes[index]
			if route.RequestBodyMode != holder.Tag.RequestBodyMode || route.Internal != holder.Tag.Internal || route.APIKeyHeader != holder.Tag.APIKeyHeader || route.APIKeyValue != holder.Tag.APIKeyValue || (len(route.MCP) != len(holder.Tag.MCP) || len(route.MCP) > 0 && !reflect.DeepEqual(route.MCP, holder.Tag.MCP)) {
				return fmt.Errorf("borrow_sql_row actual owner route metadata differs from fresh source")
			}
			mutation := holder.Tag.Settings.Mutation
			if mutation != "post" && mutation != "put" && mutation != "patch" {
				return fmt.Errorf("borrow_sql_row owner has no native physical write operation")
			}
			if ownerMutation != "" && ownerMutation != mutation {
				return fmt.Errorf("borrow_sql_row owner mutation metadata is ambiguous")
			}
			ownerMutation = mutation
			matchedRoutes[index] = true
			matches++
		}
		if matches != len(freshOwner.Component.Routes) {
			return fmt.Errorf("borrow_sql_row complete owner component holder is missing/ambiguous: %d holders for %d canonical routes", matches, len(freshOwner.Component.Routes))
		}
		if entry.helperInput == nil {
			helperInput := entry.input
			helperInput.Component = entry.input.Component.Clone()
			if err := helperInput.ValidateLifecycleTarget(true); err != nil {
				return err
			}
			initialHelpers := entry.plan
			derivedHelpers, err := (&handlercompiler.Compiler{}).BuildInput(handlercompiler.Request{Component: helperInput.Component, ViewBindings: helperInput.ViewBindings, Operation: WriteOperation(ownerMutation)}, initialHelpers.RootViewType)
			if err != nil {
				return err
			}
			helperCopy := *freshOwner
			helperCopy.Component, helperCopy.ViewBindings = derivedHelpers.Component, derivedHelpers.ViewBindings

			helperDeclarations, err := newDeclarationCompiler(helperCopy.Component, nil).compile()
			if err != nil {
				return err
			}
			helperCopy.Declarations = helperDeclarations.generation
			helperInput = rebindBorrowedInput(entry.input, &helperCopy)
			helperOptions := Options{Contracts: ContractsAuto, Handler: HandlerOptions{Target: HandlerGo, Operation: WriteOperation(ownerMutation), Input: derivedHelpers.Input, Output: derivedHelpers.Output, Currents: derivedHelpers.Currents, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}
			helperGeneration := handlergen.New((&helperCopy).handlerContext(), &helperInput, helperOptions)
			helperGeneration.Directory = location.Dir
			if err := helperGeneration.Prepare(); err != nil {
				return fmt.Errorf("borrow_sql_row native owner helper derivation: %w", err)
			}
			entry.selection, err = gen.SelectBorrowedNativeOwner(helperInput, location.Dir)
			if err != nil {
				return err
			}
			entry.helperInput = &helperInput
		}
		proof.Helpers, err = entry.selection.Helpers(declaration.Type, owner.Auxiliary)
		if err != nil {
			return err
		}
		validatedArtifacts := entry.selection.Artifacts
		for _, file := range validatedArtifacts {
			if err = gen.RetainBorrowedSeal(proof, file); err != nil {
				return err
			}
		}
		if err := sealBorrowedOwnerFiles(proof, entry.result); err != nil {
			return err
		}
		proof.OwnerOperation = ownerMutation
		borrowerCopy := *source
		frozen := append([]gen.BorrowedAuthorityFile(nil), files...)
		proof.ValidateSchema = func() error {
			refreshed, err := preparation.refresh(ctx, frozen, declarations)
			if err != nil {
				return err
			}
			currentOwner := refreshed.owners[borrowedOwnerKey(source.BaseDir(), declaration)].result
			currentDependencies := refreshed.dependencies
			currentNamed, err := borrowedNamedContracts(currentDependencies)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(admittedDependencies, currentNamed) {
				return fmt.Errorf("borrow_sql_row fresh named dependency contracts differ")
			}
			currentBorrower, err := compileBorrowedSource(ctx, &borrowerCopy, borrowerCopy.Text, frozen, freshBorrower.borrowedPreparation.foreign, currentDependencies)
			if err == nil {
				input := rebindBorrowedInput(*freshBorrower.borrowedPreparation.input, currentBorrower)
				currentBorrower.borrowedPreparation.input = &input
			}
			if err != nil {
				return err
			}
			o, _, err := bodygraph.ResolveBorrowed(currentOwner.Component, declaration.OwnerBodyPath)
			if err != nil {
				return err
			}
			b, _, err := bodygraph.ResolveBorrowed(currentBorrower.Component, declaration.BodyPath)
			if err != nil {
				return err
			}
			if err := column.ApplyWriterMetadata(currentOwner.Component); err != nil {
				return err
			}
			if err := column.ApplyWriterMetadata(currentBorrower.Component); err != nil {
				return err
			}
			oe, err := borrowedLeafContract(ctx, currentOwner, o, declaration)
			if err != nil {
				return err
			}
			be, err := borrowedLeafContract(ctx, currentBorrower, b, declaration)
			if err != nil {
				return err
			}
			if err := gen.CompareBorrowedLeafContracts(expected, oe); err != nil {
				return err
			}
			return gen.CompareBorrowedLeafContracts(expected, be)
		}
		if err := proof.ValidateFiles(func(path string) string { return path }); err != nil {
			return err
		}
		actualSlot.TypeName = declaration.Type
		id, _ := actualSlot.Identity()
		if result.Views == nil {
			result.Views = gen.ViewReferences{}
		}
		if result.Views[id] != nil {
			return fmt.Errorf("borrow_sql_row slot already has separate linked authority")
		}
		result.Views[id] = &gen.ViewReference{DescriptorKey: row.Key(), Borrowed: proof}
		result.TypeResolver = resolver
	}
	return nil
}

func sealBorrowedOwnerFiles(proof *gen.BorrowedRowAuthority, owners ...*Result) error {
	var foreign *typecatalog.Catalog
	var snapshot *dependency.Snapshot
	if len(owners) != 0 && owners[0] != nil {
		foreign = owners[0].borrowedPreparation.foreign
		snapshot = owners[0].Source.dependencySnapshot
	}
	scalars, err := borrowedScalarDependencies(proof.Expected, foreign)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(proof.OwnerDirectory)
	if err != nil {
		return err
	}
	found := map[string]int{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(proof.OwnerDirectory, entry.Name())
		sealed, err := gen.SealBorrowedAuthorityFile(path)
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, sealed.Bytes, parser.ParseComments)
		if err != nil {
			return err
		}
		owned := strings.Contains(string(sealed.Bytes), "// Code generated by Datly for "+proof.OwnerName+"; DO NOT EDIT.")
		if capturedBorrowedScalarFile(snapshot, path) && borrowedScalarSource(parsed, scalars) {
			if err := retainBorrowedScalarSource(proof, snapshot, sealed); err != nil {
				return err
			}
		}
		for _, decl := range parsed.Decls {
			g, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, s := range g.Specs {
				ts, ok := s.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if ts.Name.Name == proof.Expected.Name || ts.Name.Name == proof.Expected.Name+"Has" {
					if !owned {
						return fmt.Errorf("borrow_sql_row row/Has declaration is not owned by exact generated component")
					}
					found[ts.Name.Name]++
					if ts.Name.Name == proof.Expected.Name {
						proof.RowFile = path
					} else {
						proof.HasFile = path
					}
				}
			}
		}
		if owned {
			for _, decl := range parsed.Decls {
				method, ok := decl.(*ast.FuncDecl)
				if !ok || method.Recv == nil || len(method.Recv.List) != 1 {
					continue
				}
				typ := method.Recv.List[0].Type
				if ptr, ok := typ.(*ast.StarExpr); ok {
					typ = ptr.X
				}
				receiver, ok := typ.(*ast.Ident)
				if !ok {
					continue
				}
				for i := range proof.Helpers {
					helper := &proof.Helpers[i]
					if receiver.Name == helper.Receiver && method.Name.Name == helper.Name {
						if helper.Path != "" {
							return fmt.Errorf("borrow_sql_row owner helper is ambiguous")
						}
						helper.Path = path
					}
				}
			}
			if err := gen.RetainBorrowedSeal(proof, sealed); err != nil {
				return err
			}
			if strings.Contains(string(sealed.Bytes), "//go:embed") {
				assets, err := packageasset.SourceEmbedFiles(proof.OwnerDirectory, sealed.Bytes)
				if err != nil {
					return err
				}
				for _, asset := range assets {
					resourceFile, err := gen.SealBorrowedAuthorityFile(filepath.Join(proof.OwnerDirectory, filepath.FromSlash(asset)))
					if err != nil {
						return err
					}
					if err := gen.RetainBorrowedSeal(proof, resourceFile); err != nil {
						return err
					}
				}
			}
		}
	}
	if found[proof.Expected.Name] != 1 || found[proof.Expected.Name+"Has"] != 1 {
		return fmt.Errorf("borrow_sql_row actual row/Has declaration is missing or ambiguous")
	}
	sort.Slice(proof.Files, func(i, j int) bool { return proof.Files[i].Path < proof.Files[j].Path })
	return nil
}

// applyBorrowedPhysicalFacts binds independently copied native evidence to the
// existing native emission; it never emits a replacement row or changes tags.
func applyBorrowedPhysicalFacts(contract *gen.BorrowedLeafContract, facts column.PhysicalSourceConstraints) error {
	if contract == nil || facts.Identity.Connector != contract.Connector || facts.Identity.Catalog != contract.Catalog || facts.Identity.Schema != contract.Schema || facts.Identity.Table != contract.Table {
		return fmt.Errorf("borrow_sql_row physical contract identity conflicts")
	}
	facts.Projection = append([]column.PhysicalProjectionFact(nil), facts.Projection...)
	columns := map[string]column.PhysicalColumnFact{}
	for _, c := range facts.NativeColumns {
		columns[c.Name] = c
	}
	fields := map[string]gen.Field{}
	for _, f := range contract.Fields {
		if _, exists := fields[f.Name]; exists {
			return fmt.Errorf("borrow_sql_row generated field is ambiguous")
		}
		fields[f.Name] = f
	}
	for i, p := range facts.Projection {
		f, ok := fields[typecatalog.FieldName(p.Projected)]
		if !ok {
			return fmt.Errorf("borrow_sql_row generated physical field is absent")
		}
		t := sqlio.ParseTag(reflect.StructTag(f.Tag))
		mapping := strings.Split(t.Column, "|")
		if t.Transient || len(mapping) == 0 || len(mapping) > 2 || mapping[0] != p.Physical {
			return fmt.Errorf("borrow_sql_row actual native field physical mapping conflicts")
		}
		if len(mapping) == 2 && mapping[1] != p.Result {
			return fmt.Errorf("borrow_sql_row actual native field result mapping conflicts")
		}
		if !strings.EqualFold(p.Result, p.Physical) && !strings.EqualFold(p.Result, f.Name) && (len(mapping) != 2 || mapping[1] != p.Result) {
			return fmt.Errorf("borrow_sql_row actual native field loses SQL result alias")
		}
		c := columns[p.Physical]
		primary := c.Primary
		unique := !primary && strings.EqualFold(c.Key, "UNI")
		auto := c.AutoIncrement != nil && *c.AutoIncrement
		if c.Default != nil {
			v := strings.ToLower(*c.Default)
			auto = auto || strings.Contains(v, "autoincrement") || strings.Contains(v, "auto_increment")
		}
		if t.PrimaryKey != primary || t.Autoincrement != auto || t.IsUnique != unique {
			return fmt.Errorf("borrow_sql_row actual native field key/autoincrement/unique metadata conflicts with physical facts")
		}
		references := 0
		for _, k := range facts.ForeignKeys {
			if k.Column != p.Physical {
				continue
			}
			references++
			schema := k.ReferenceSchema
			if facts.Product == "SQLite" && schema == "" {
				schema = facts.Identity.Schema
			}
			expectedDb := schema
			if schema == facts.Identity.Schema {
				expectedDb = ""
			}
			if t.RefDb != expectedDb || t.RefTable != k.ReferenceTable || t.RefColumn != k.ReferenceColumn {
				return fmt.Errorf("borrow_sql_row actual native field reference override conflicts with physical facts")
			}
		}
		if references == 0 && (t.RefDb != "" || t.RefTable != "" || t.RefColumn != "") {
			return fmt.Errorf("borrow_sql_row actual native field invents physical reference")
		}
		facts.Projection[i].Mapping = t.Column
	}
	serialized, err := json.Marshal(facts)
	if err != nil {
		return err
	}
	contract.Physical = serialized
	return nil
}

// borrowedPreparation retains invocation-local owner products. Publication refreshes
// schema facts from captured authoring and destination context, never success state.
type borrowedPreparation struct {
	input        *gen.Input
	plan         *gen.Plan
	foreign      *typecatalog.Catalog
	owners       map[string]*borrowedOwnerPreparation
	dependencies *typecatalog.Catalog
	baseDir      string
}

type borrowedOwnerPreparation struct {
	source      Source
	result      *Result
	input       gen.Input
	plan        *gen.Plan
	helperInput *gen.Input
	selection   *gen.BorrowedNativeOwnerSelection
}

func borrowedOwnerKey(baseDir string, d spec.BorrowedSQLRow) string {
	return d.Package + "\x00" + filepath.Join(baseDir, filepath.FromSlash(d.OwnerURI)) + "\x00" + d.OwnerName
}

func prepareBorrowedOwners(ctx context.Context, source *Source, component *spec.Component, files []gen.BorrowedAuthorityFile) (*borrowedPreparation, error) {
	preparation := &borrowedPreparation{owners: map[string]*borrowedOwnerPreparation{}, dependencies: typecatalog.NewCatalog(), baseDir: source.BaseDir()}
	catalog := preparation.dependencies

	for _, declaration := range authoredBorrowedSQLRows(component) {
		if err := declaration.Validate(); err != nil {
			return nil, err
		}
		key := borrowedOwnerKey(source.BaseDir(), declaration)
		entry := preparation.owners[key]
		if entry == nil {
			ownerPath := filepath.Join(source.BaseDir(), filepath.FromSlash(declaration.OwnerURI))
			if filepath.Clean(ownerPath) == filepath.Clean(source.Path) || strings.TrimSuffix(filepath.Base(ownerPath), filepath.Ext(ownerPath)) != declaration.OwnerName {
				return nil, fmt.Errorf("borrow_sql_row owner must identify a different exact Source.Name and resource")
			}
			var ownerBytes []byte
			for _, file := range files {
				if file.Path == ownerPath {
					ownerBytes = file.Bytes
				}
			}
			if ownerBytes == nil {
				return nil, fmt.Errorf("borrow_sql_row owner DQL resource is missing")
			}
			ownerSource := *source
			ownerSource.Name = declaration.OwnerName
			ownerSource.Path = ownerPath
			ownerSource.Text = string(ownerBytes)
			relative, err := filepath.Rel(source.BaseDir(), filepath.Dir(ownerPath))
			if err != nil {
				return nil, err
			}
			if relative != "." {
				ownerSource.Scope = strings.TrimSuffix(source.Scope, "/") + "/" + filepath.ToSlash(relative)
			}
			owner, err := freshBorrowedSource(ctx, &ownerSource, string(ownerBytes), files)
			if err != nil {
				return nil, err
			}
			if len(authoredBorrowedSQLRows(owner.Component)) != 0 {
				return nil, fmt.Errorf("borrow_sql_row borrowed owners/cyclic dependencies are unsupported")
			}
			if owner.Component.TypeContext == nil || owner.Component.TypeContext.PackagePath != declaration.Package {
				return nil, fmt.Errorf("borrow_sql_row owner canonical package differs")
			}
			input, err := borrowedGenerationInput(ctx, owner, declaration.Package)
			if err != nil {
				return nil, err
			}
			plan, err := gen.BorrowedOwnerPlan(input)
			if err != nil {
				return nil, err
			}
			owner.borrowedPreparation.input = &input
			owner.borrowedPreparation.plan = plan
			entry = &borrowedOwnerPreparation{source: ownerSource, result: owner, input: input, plan: plan}
			preparation.owners[key] = entry
		}
		types, err := gen.BorrowedNamedDependenciesFromPlan(entry.plan, entry.input.TargetPackage, declaration.Type)
		if err != nil {
			return nil, fmt.Errorf("borrow_sql_row fresh owner named closure: %w", err)
		}
		for _, typ := range types {
			if err := catalog.Register(typecatalog.TypeOriginDQL, typ); err != nil {
				return nil, err
			}
		}
	}
	return preparation, nil
}

// borrowedPhysicalProjection removes explicitly transient outputs from a
// detached analysis projection only. Their exact types/tags/expressions remain
// in the row comparison; they supply no physical lineage evidence.
func borrowedPhysicalProjection(view *spec.View) error {
	if strings.TrimSpace(view.Source.SQL) == "" {
		return nil
	}
	transient := map[string]bool{}
	var persisted []*spec.Column
	for _, c := range view.Columns {
		if c != nil && sqlio.ParseTag(reflect.StructTag(c.Tag)).Transient {
			name := strings.TrimSpace(c.Source)
			if name == "" {
				name = strings.TrimSpace(c.Name)
			}
			if name != "" {
				transient[strings.ToLower(name)] = true
			}
			if output := strings.TrimSpace(c.Output); output != "" {
				transient[strings.ToLower(output)] = true
			}
			continue
		}
		persisted = append(persisted, c)
	}
	_, query, err := sqlanalysis.Checked(view.Source.SQL, nil)
	if err != nil {
		return err
	}
	// The compiler retains a pass-through SELECT * wrapper around an authored
	// SQL body. A sole qualified star must name that exact derived FROM alias.
	// Peel only those projections; arbitrary nested SQL is still
	// rejected by PhysicalSourceConstraints.
	for len(query.List) == 1 && len(query.Joins) == 0 && query.Union == nil && len(query.WithSelects) == 0 && !query.WithRecursive && query.Kind == "" && query.Qualify == nil && len(query.GroupBy) == 0 && query.Having == nil && query.QualifyClause == nil && query.Window == nil && len(query.OrderBy) == 0 && query.Limit == nil && query.Offset == nil {
		star, ok := query.List[0].Expr.(*sqlexpr.Star)
		if !ok || len(star.Except) != 0 {
			break
		}
		transparentStar := false
		switch projection := star.X.(type) {
		case *sqlexpr.Ident:
			transparentStar = projection.Name == "*"
		case *sqlexpr.Selector:
			name, plain := projection.X.(*sqlexpr.Ident)
			transparentStar = plain && name.Name == "*" && projection.Expression == "" && query.From.Alias != "" && projection.Name == query.From.Alias
		}
		if !transparentStar || query.List[0].Alias != "" {
			break
		}
		nested := query.NestedSelect()
		if nested == nil {
			break
		}
		query = nested
	}
	if view.Auxiliary {
		table, parenthesized, err := sqlparser.SourceTable(query.From.X)
		if err != nil {
			return err
		}
		if parenthesized && table == view.Source.Table {
			query.From.X = &sqlexpr.Ident{Name: table}
		}
	}
	filtered := query.List[:0]
	for _, item := range query.List {
		if item == nil {
			return fmt.Errorf("borrow_sql_row physical projection item is absent")
		}
		column := sqlparser.NewColumn(item)
		if transient[strings.ToLower(column.Identity())] {
			continue
		}
		filtered = append(filtered, item)
	}
	query.List = filtered
	view.Source.SQL = sqlparser.Stringify(query)
	view.Columns = persisted
	return nil
}

func mergeBorrowedOwnerDependencies(existing, fresh *typecatalog.Catalog) (*typecatalog.Catalog, error) {
	merged := typecatalog.NewCatalog()
	if existing != nil {
		var err error
		merged, err = existing.Clone()
		if err != nil {
			return nil, err
		}
	}
	registry, err := fresh.Registry(typecatalog.TranscribeAuthority)
	if err != nil {
		return nil, err
	}
	registry.ForEach(func(key string, typ *x.Type) bool {
		origin, _, originErr := fresh.Origin(typecatalog.TranscribeAuthority, key)
		if originErr != nil {
			err = originErr
			return false
		}
		if origin == typecatalog.TypeOriginDQL {
			err = merged.Register(origin, typ)
		}
		return err == nil
	})
	return merged, err
}

func borrowedNamedContracts(catalog *typecatalog.Catalog) (map[string][]gen.Field, error) {
	registry, err := catalog.Registry(typecatalog.TranscribeAuthority)
	if err != nil {
		return nil, err
	}
	result := map[string][]gen.Field{}
	registry.ForEach(func(key string, typ *x.Type) bool {
		origin, _, originErr := catalog.Origin(typecatalog.TranscribeAuthority, key)
		if originErr != nil {
			err = originErr
			return false
		}
		if origin == typecatalog.TypeOriginDQL {
			result[key], err = gen.BorrowedNamedFields(typ)
		}
		return err == nil
	})
	return result, err
}

// Load explicit imports through native package authority. Keep owner-authored
// named scalars, while fresh DQL remains the only authority for owner row/Has
// and structured dependencies. The captured snapshot is reused at revalidation.
func freshBorrowedForeignTypes(ctx context.Context, source *Source, text string) (*typecatalog.Catalog, error) {
	prepared := dql.PrepareSource(text)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	catalog := typecatalog.NewCatalog()
	if prepared.TypeContext == nil {
		return catalog, nil
	}
	owner := prepared.TypeContext.PackagePath
	workspace, err := (xmodule.LocalWorkspace{BaseDir: source.BaseDir()}).Resolve(ctx)
	if err != nil {
		return nil, err
	}
	staged := typecatalog.NewCatalog()
	if source.dependencySnapshot == nil {
		source.dependencySnapshot = &dependency.Snapshot{}
	}
	loader := dependency.Loader{Workspace: workspace, Catalog: staged, Snapshot: source.dependencySnapshot}
	imports := map[string]bool{}
	for _, imp := range prepared.TypeContext.Imports {
		imports[imp.Package] = true
	}
	if _, err := loader.LoadImports(ctx, imports); err != nil {
		return nil, err
	}
	component := (&spec.Component{Parameters: prepared.Directives.Params, Views: prepared.Directives.Views}).Clone()
	if err := loader.LoadCodecs(ctx, component, link.TypeContext(source.Scope, "", prepared.TypeContext)); err != nil {
		return nil, err
	}
	ownerDirectory := ""
	if source.dependencySnapshot.Packages[owner] != nil {
		location, err := workspace.Package(owner)
		if err != nil {
			return nil, err
		}
		if location != nil {
			ownerDirectory = location.Dir
		}
	}
	scalars := borrowedAuthoredScalarNames(source.dependencySnapshot, owner, ownerDirectory)
	registry, err := staged.Registry(typecatalog.TranscribeAuthority)
	if err != nil {
		return nil, err
	}
	registry.ForEach(func(key string, typ *x.Type) bool {
		if typ.PkgPath == owner && !scalars[typ.Name] {
			return true
		}
		origin, _, originErr := staged.Origin(typecatalog.TranscribeAuthority, key)
		if originErr != nil {
			err = originErr
			return false
		}
		err = catalog.Register(origin, typ)
		return err == nil
	})
	return catalog, err
}

// Reuse ordinary destination, contract-name and resource preparation for every
// fresh owner proof plan. Direct metadata-only tests have no project destination.
func borrowedGenerationInput(ctx context.Context, result *Result, packagePath string) (gen.Input, error) {
	if result.borrowedPreparation != nil && result.borrowedPreparation.input != nil {
		return rebindBorrowedInput(*result.borrowedPreparation.input, result), nil
	}
	if result.Source.BaseDir() == "" || result.Component.TypeContext == nil || result.Component.TypeContext.PackagePath == "" {
		return gen.Input{Component: result.Component, TargetPackage: packagePath, TypeResolver: result.TypeResolver, Declarations: result.Declarations, ViewBindings: result.ViewBindings}, nil
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: result.Source.BaseDir()}).Resolve(ctx)
	if err != nil {
		return gen.Input{}, err
	}
	location, err := workspace.Package(packagePath)
	if err != nil {
		return gen.Input{}, err
	}
	if location == nil {
		return gen.Input{}, fmt.Errorf("borrow_sql_row owner package destination is missing")
	}
	relative, err := filepath.Rel(location.Module.Dir, location.Dir)
	if err != nil {
		return gen.Input{}, err
	}
	input, _, err := generationInput(location.Module.Dir, relative, result)
	if err == nil {
		if result.borrowedPreparation == nil {
			result.borrowedPreparation = &borrowedPreparation{}
		}
		result.borrowedPreparation.input = &input
	}
	return input, err
}

func rebindBorrowedInput(input gen.Input, result *Result) gen.Input {
	input.Component = result.Component.Clone()
	input.TypeResolver = result.TypeResolver
	input.Declarations = result.Declarations
	input.ViewBindings = result.ViewBindings
	return input
}

func (p *borrowedPreparation) refresh(ctx context.Context, files []gen.BorrowedAuthorityFile, declarations []spec.BorrowedSQLRow) (*borrowedPreparation, error) {
	refreshed := &borrowedPreparation{owners: map[string]*borrowedOwnerPreparation{}, dependencies: typecatalog.NewCatalog(), baseDir: p.baseDir}
	for _, declaration := range declarations {
		key := borrowedOwnerKey(p.baseDir, declaration)
		entry := refreshed.owners[key]
		if entry == nil {
			captured := p.owners[key]
			result, err := compileBorrowedSource(ctx, &captured.source, captured.source.Text, files, captured.result.borrowedPreparation.foreign)
			if err != nil {
				return nil, err
			}
			input := rebindBorrowedInput(captured.input, result)
			result.borrowedPreparation.input = &input
			plan, err := gen.BorrowedOwnerPlan(input)
			if err != nil {
				return nil, err
			}
			result.borrowedPreparation.plan = plan
			entry = &borrowedOwnerPreparation{source: captured.source, result: result, input: input, plan: plan}
			refreshed.owners[key] = entry
		}
		types, err := gen.BorrowedNamedDependenciesFromPlan(entry.plan, entry.input.TargetPackage, declaration.Type)
		if err != nil {
			return nil, err
		}
		for _, typ := range types {
			if err := refreshed.dependencies.Register(typecatalog.TypeOriginDQL, typ); err != nil {
				return nil, err
			}
		}
	}
	return refreshed, nil
}
