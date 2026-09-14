package generate

import (
	"bytes"
	"context"
	"fmt"
	"go/format"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"

	afsembed "github.com/viant/afs/embed"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

// ResolveEntityHookTypes loads the actual create-once file when it exists, or
// its proposed source otherwise. The detached native package preserves the real
// canonical identity for the orchestration owner's hook compilation.
func (a *HookScaffoldAsset) ResolveEntityHookTypes(directory string) (*typecatalog.Resolver, error) {
	if a == nil || a.PackagePath == "" || a.File == nil || a.File.Name == nil || len(a.EntityHooks) == 0 {
		return nil, fmt.Errorf("mutation hook scaffold requires package, source and typed contracts")
	}
	holder := afsembed.NewHolder()
	if directory != "" {
		if _, err := os.Stat(directory); err == nil {
			snapshot, err := (packageasset.Snapshotter{Source: os.DirFS(directory)}).All(context.Background())
			if err != nil {
				return nil, err
			}
			if err = fs.WalkDir(snapshot, ".", func(name string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil || entry.IsDir() {
					return walkErr
				}
				content, err := fs.ReadFile(snapshot, name)
				if err != nil {
					return err
				}
				holder.Add(name, string(content))
				return nil
			}); err != nil {
				return nil, err
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	// This module header exists only in memory to give the native source loader
	// the package identity; no application module or source file is written.
	holder.Add("go.mod", "module "+a.PackagePath+"\n")
	name := a.Destination
	if name == "" {
		name = "hooks.go"
	}
	if _, err := holder.EmbedFs().ReadFile(name); os.IsNotExist(err) {
		var source bytes.Buffer
		if err = format.Node(&source, token.NewFileSet(), a.File); err != nil {
			return nil, err
		}
		holder.Add(name, source.String())
	} else if err != nil {
		return nil, err
	}
	pkg, err := loaderast.LoadPackageFS(context.Background(), holder.EmbedFs(), ".")
	if err != nil {
		return nil, fmt.Errorf("load mutation hook scaffold: %w", err)
	}
	for _, file := range pkg.Files {
		if file.PkgName != a.File.Name.Name {
			return nil, fmt.Errorf("mutation hook package %s differs from generated package %s", file.PkgName, a.File.Name.Name)
		}
	}
	catalog := typecatalog.NewCatalog()
	if a.Catalog != nil {
		catalog, err = a.Catalog.Clone()
		if err != nil {
			return nil, err
		}
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginGenerated, pkg); err != nil {
		return nil, err
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{DefaultPackage: a.PackagePath, PackagePath: a.PackagePath})
	if err != nil {
		return nil, err
	}
	return resolver, nil
}

func (p *Plan) validateMutationHookScaffold() error {
	hook := p.HookScaffold
	asset := &HookScaffoldAsset{Destination: hook.Destination, File: hook.File, PackagePath: hook.PackagePath, EntityHooks: hook.EntityHooks, Catalog: hook.Catalog, evidence: hook.evidence}
	return asset.validateMutationMethods("")
}

func (p *Plan) validateExistingMutationHookScaffold(path string) error {
	hook := p.HookScaffold
	asset := &HookScaffoldAsset{Destination: filepath.Base(path), File: hook.File, PackagePath: hook.PackagePath, EntityHooks: hook.EntityHooks, Catalog: hook.Catalog, evidence: hook.evidence}
	return asset.validateMutationMethods(filepath.Dir(path))
}

// RetainEntityHookEvidence snapshots canonical native method sets after the
// orchestration owner has approved them with EntityHookCompiler. This method
// does not establish SDK compatibility. It must receive independent compiled
// results, never signatures inferred from an unvalidated mutable proposal.
func (a *HookScaffoldAsset) RetainEntityHookEvidence(methods map[string][]xshape.Method) error {
	if a == nil || a.File == nil || a.File.Name == nil || a.PackagePath == "" || len(a.EntityHooks) == 0 || len(methods) != len(a.EntityHooks) {
		return fmt.Errorf("mutation hook scaffold requires compiler-approved native method/role evidence")
	}
	if a.evidence != nil {
		return fmt.Errorf("mutation hook scaffold evidence is already retained")
	}
	evidence := &hookScaffoldEvidence{packagePath: a.PackagePath, packageName: a.File.Name.Name}
	seen := map[string]bool{}
	for _, contract := range a.EntityHooks {
		if contract.Type == "" || contract.Identity == "" || len(contract.Path) == 0 || len(contract.Required) == 0 || seen[contract.Type] {
			return fmt.Errorf("mutation hook scaffold requires unique typed roles and mandatory methods")
		}
		seen[contract.Type] = true
		signatures := methods[contract.Type]
		names := map[string]bool{}
		for _, method := range signatures {
			if method.Name == "" || names[method.Name] {
				return fmt.Errorf("mutation hook %s has invalid native method evidence", contract.Type)
			}
			names[method.Name] = true
		}
		for _, name := range contract.Required {
			if !names[name] {
				return fmt.Errorf("mutation hook %s evidence requires %s", contract.Type, name)
			}
		}
		evidence.roles = append(evidence.roles, hookScaffoldRole{contract: contract, methods: signatures})
	}
	a.evidence = evidence.clone()
	return nil
}

type hookScaffoldEvidence struct {
	packagePath, packageName string
	roles                    []hookScaffoldRole
}

type hookScaffoldRole struct {
	contract HookScaffoldContract
	methods  []xshape.Method
}

func (e *hookScaffoldEvidence) clone() *hookScaffoldEvidence {
	if e == nil {
		return nil
	}
	result := &hookScaffoldEvidence{packagePath: e.packagePath, packageName: e.packageName, roles: make([]hookScaffoldRole, len(e.roles))}
	for i, role := range e.roles {
		result.roles[i].contract = role.contract.clone()
		result.roles[i].methods = append([]xshape.Method(nil), role.methods...)
		for j := range result.roles[i].methods {
			method := &result.roles[i].methods[j]
			method.Parameters = append([]string(nil), method.Parameters...)
			method.Results = append([]string(nil), method.Results...)
		}
	}
	return result
}

func (a *HookScaffoldAsset) validateMutationMethods(directory string) error {
	evidence := a.evidence
	if evidence == nil || len(evidence.roles) == 0 {
		return fmt.Errorf("mutation hook scaffold requires compiler-approved native method/role evidence")
	}
	if a.File == nil || a.File.Name == nil || a.PackagePath != evidence.packagePath || a.File.Name.Name != evidence.packageName || len(a.EntityHooks) != len(evidence.roles) {
		return fmt.Errorf("mutation hook scaffold package or role contract changed after compilation")
	}
	for i, role := range evidence.roles {
		if !reflect.DeepEqual(a.EntityHooks[i], role.contract) {
			return fmt.Errorf("mutation hook scaffold role contract changed after compilation")
		}
	}
	actual, err := a.ResolveEntityHookTypes(directory)
	if err != nil {
		return err
	}
	for _, role := range evidence.roles {
		contract := role.contract
		currentType, err := actual.Descriptor(contract.Type)
		if err != nil || currentType == nil {
			return fmt.Errorf("mutation hook scaffold type %s is missing: %v", contract.Type, err)
		}
		current, err := xshape.New(currentType, actual.Descriptor).Methods(true)
		if err != nil {
			return err
		}
		byName := map[string]xshape.Method{}
		for _, method := range current {
			byName[method.Name] = method
		}
		for _, name := range contract.Required {
			if _, ok := byName[name]; !ok {
				return fmt.Errorf("mutation hook %s requires %s", contract.Type, name)
			}
		}
		for _, method := range role.methods {
			if got, found := byName[method.Name]; found && !reflect.DeepEqual(method, got) {
				return fmt.Errorf("mutation hook %s.%s has an incompatible signature against compiler-approved evidence", contract.Type, method.Name)
			}
		}
	}
	return nil
}
