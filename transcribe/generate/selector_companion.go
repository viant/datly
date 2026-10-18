package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// selectorSourceOwnership uses current package declarations, never a previous
// association or a persisted fingerprint, to distinguish authored companions.
func (r *planResolver) prepareSelectorOwnership() error {
	p := r.plan
	if len(p.QuerySelectors) == 0 {
		return nil
	}
	if r.input.ProjectRoot == "" || p.Package == "" {
		return r.preserveLinkedSelectors(false)
	}
	authority, err := typecatalog.NewDestinationAuthority(r.input.ProjectRoot)
	if err != nil {
		return err
	}
	destination, err := authority.Resolve(p.Package, "")
	if err != nil {
		return err
	}
	directory := filepath.Join(r.input.ProjectRoot, destination.Directory)
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return r.preserveLinkedSelectors(false)
	}
	if err != nil {
		return err
	}
	projection := map[string][]byte{}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			body, err := os.ReadFile(filepath.Join(directory, entry.Name()))
			if err != nil {
				return err
			}
			projection[filepath.Join(directory, entry.Name())] = body
		}
	}
	if len(projection) == 0 {
		return r.preserveLinkedSelectors(false)
	}
	build := r.input.GoBuild
	if build == nil {
		build = &gobuild.Context{Dir: r.input.ProjectRoot}
	}
	selected, err := build.GoFiles(p.Package, projection)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, name := range selected {
		active[filepath.Base(name)] = true
	}
	generatedInput := false
	methods := 0
	for _, entry := range entries {
		if !active[entry.Name()] {
			continue
		}
		body, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), body, 0)
		if err != nil {
			if generatedOwner(body) == p.ComponentName {
				continue
			}
			return err
		}
		owned := generatedOwner(body) == p.ComponentName
		for _, declaration := range file.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "DatlyQuerySelectors" && function.Recv != nil && len(function.Recv.List) == 1 {
				receiver := function.Recv.List[0].Type
				if pointer, ok := receiver.(*ast.StarExpr); ok {
					receiver = pointer.X
				}
				if name, ok := receiver.(*ast.Ident); ok && name.Name == p.HolderName() {
					methods++
					p.ExistingQuerySelectors = !owned
				}
			}
			group, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, item := range group.Specs {
				typ, ok := item.(*ast.TypeSpec)
				if !ok || typ.Name.Name != p.Input.Type {
					continue
				}
				_, ok = typ.Type.(*ast.StructType)
				if !ok {
					continue
				}
				if owned {
					generatedInput = true
					continue
				}

			}
		}
	}
	if methods > 1 {
		return fmt.Errorf("duplicate current selector companion declarations")
	}
	return r.preserveLinkedSelectors(generatedInput)
}

func (r *planResolver) preserveLinkedSelectors(generated bool) error {
	p := r.plan
	if p.Input.Ownership != ContractLinked {
		return nil
	}
	if generated && p.Input.Package == p.Package {
		p.Input.Ownership = ContractGenerated
		return nil
	}
	side, err := r.linkedSelectorFields()
	if err != nil {
		return err
	}
	p.QuerySelectors = side
	planned, err := resolveInputFieldsMode(r.input.Component, r.input.Declarations, false)
	if err != nil {
		return err
	}
	remaining := map[string]bool{}
	for _, selector := range side {
		remaining[selector.Name] = true
	}
	p.Input.Fields = nil
	for _, field := range planned {
		if !remaining[field.Name] {
			p.Input.Fields = append(p.Input.Fields, field)
		}
	}
	return nil
}

// linkedSelectorFields classifies side storage against the exact admitted Input
// without resolving unrelated output or independent-view graph ownership.
func (r *planResolver) linkedSelectorFields() ([]Field, error) {
	p := r.plan
	descriptor, err := r.types.Descriptor(p.Input.DescriptorKey)
	if err != nil {
		return nil, err
	}
	fields, err := xshape.New(descriptor, r.types.Descriptor).Fields()
	if err != nil {
		return nil, err
	}
	side := make([]Field, 0, len(p.QuerySelectors))
	for _, selector := range p.QuerySelectors {
		wanted, err := tag.ParseField(reflect.StructField{Name: selector.Name, Tag: reflect.StructTag(selector.Tag)})
		if err != nil {
			return nil, err
		}
		direct := false
		for _, field := range fields {
			actual, err := tag.ParseField(field.StructField())
			if err != nil {
				return nil, err
			}
			if actual.Binding != nil && actual.QuerySelector != nil && wanted.Binding != nil && wanted.QuerySelector != nil && actual.Binding.Location.Kind == wanted.Binding.Location.Kind && actual.Binding.Location.In == wanted.Binding.Location.In && actual.QuerySelector.View == wanted.QuerySelector.View && actual.QuerySelector.Property == wanted.QuerySelector.Property {
				direct = true
				break
			}
		}
		if !direct {
			side = append(side, selector)
		}
	}
	return side, nil
}
