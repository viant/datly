package bootstrap

import (
	"context"
	"fmt"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"io/fs"
	"os"
	"reflect"
	"strings"
)

func (s *RouteSource) resolveReaderMetadata(input, output *x.Type, types *typecatalog.Resolver) (*spec.Component, error) {
	if s.Tag.ReaderMetadata == "" {
		return nil, nil
	}
	namespace, path, qualified := strings.Cut(s.Tag.ReaderMetadata, ":")
	if !qualified || namespace == "" || !fs.ValidPath(path) || !strings.HasPrefix(path, "metadata/") {
		return nil, fmt.Errorf("invalid reader metadata resource reference")
	}
	var source fs.FS
	if s.Dir != "" {
		root, err := os.OpenRoot(s.Dir)
		if err != nil {
			return nil, err
		}
		defer root.Close()
		declarations, err := packageasset.DiscoverSourceResourcesFS(root.FS())
		if err != nil {
			return nil, err
		}
		var selected *packageasset.Resources
		for _, d := range declarations {
			if d.Namespace == namespace {
				if selected != nil {
					return nil, fmt.Errorf("duplicate reader metadata resource namespace")
				}
				selected = d
			}
		}
		if selected == nil {
			return nil, fmt.Errorf("reader metadata namespace is not declared by selected package")
		}
		found := false
		for _, p := range selected.Files {
			if p == path {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("reader metadata is not an embedded package asset")
		}
		source, err = (packageasset.Snapshotter{Source: root.FS()}).Files(context.Background(), selected.Files)
		if err != nil {
			return nil, err
		}
	} else {
		holder := linkedHolder(nil, s.PackagePath, s.HolderType)
		if s.linkedHolderType != nil {
			holder = reflect.New(s.linkedHolderType).Interface()
		}
		embedded, ok := holder.(EmbeddedResource)
		if !ok || embedded.EmbedNamespace() != namespace || embedded.EmbedFS() == nil {
			return nil, fmt.Errorf("reader metadata requires exact holder embedded resource authority")
		}
		var err error
		source, err = (packageasset.Snapshotter{Source: embedded.EmbedFS()}).All(context.Background())
		if err != nil {
			return nil, err
		}
	}
	body, err := fs.ReadFile(source, path)
	if err != nil {
		return nil, err
	}
	document, err := readergraph.StrictDecode(body)
	if err != nil {
		return nil, err
	}
	if input == nil || output == nil || document.Package != s.PackagePath || document.Holder != s.HolderType || document.Field != s.FieldName || document.Input != input.Key() || document.Output != output.Key() {
		return nil, fmt.Errorf("reader metadata exact holder or contract identity mismatch")
	}
	component := document.Component
	if component.Settings == nil {
		return nil, fmt.Errorf("reader metadata settings required")
	}
	if component.Key.Scope != s.PackagePath || component.Key.Name != s.componentName() || component.Name != s.componentName() || len(component.Routes) != 1 || component.Routes[0] == nil || component.Routes[0].Method != s.Tag.Method || component.Routes[0].Path != s.Tag.Path {
		return nil, fmt.Errorf("reader metadata route identity mismatch")
	}
	// The holder's component-wide policy must agree, rather than allowing a
	// copied metadata asset to grant an execution owner the declaration lacks.
	base, err := s.canonicalComponent()
	if err != nil {
		return nil, err
	}
	if component.Settings.InputType != input.Key() || component.Settings.OutputType != output.Key() {
		return nil, fmt.Errorf("reader metadata settings contract identity mismatch")
	}
	if base.Settings.Mutation != "" || base.Settings.SequenceStrategy != "" || base.Settings.ComponentCallPolicy != "" || base.Settings.IndependentChildTransactions || base.Settings.WarmupTarget != nil || len(base.Settings.ProtectedFlushTables) > 0 {
		return nil, fmt.Errorf("reader metadata holder has unsupported execution policy")
	}
	if base.Settings.Mutation != component.Settings.Mutation || base.Settings.ComponentCallPolicy != component.Settings.ComponentCallPolicy || s.Tag.Handler != "" {
		return nil, fmt.Errorf("reader metadata execution owner mismatch")
	}
	lookup := func(name string) (*x.Type, error) {
		if types != nil {
			if d, err := types.Descriptor(name); d != nil || err != nil {
				return d, err
			}
		}
		if input.Key() == name {
			return input, nil
		}
		if output.Key() == name {
			return output, nil
		}
		for _, typ := range []reflect.Type{input.Type, output.Type} {
			if d := findReaderRow(typ, name, map[reflect.Type]bool{}); d != nil {
				return d, nil
			}
		}
		return nil, nil
	}
	if err = readergraph.Contracts(component, input, output, lookup); err != nil {
		return nil, err
	}
	if err = readergraph.CheckRows(document, lookup); err != nil {
		return nil, err
	}
	store := resource.New()
	if err = store.Register(namespace, source); err != nil {
		return nil, err
	}
	assets := map[string][]byte{s.Tag.ReaderMetadata: body}
	recorder := readerMetadataResources{base: store, namespace: namespace, assets: assets}
	err = readergraph.WalkViews(component, func(view *spec.View) error {
		if view.Source == nil {
			return nil
		}
		if view.Source.SQL != "" || len(view.Source.Embeds) > 0 {
			return fmt.Errorf("reader metadata contains competing inline SQL")
		}
		if view.Source.URI == "" {
			return nil
		}
		ns, p, ok := strings.Cut(view.Source.URI, ":")
		if !ok || ns != namespace || !fs.ValidPath(p) {
			return fmt.Errorf("reader metadata SQL uses foreign resource authority")
		}
		data, err := fs.ReadFile(source, p)
		if err != nil {
			return err
		}
		assets[view.Source.URI] = data
		return dsql.ResolveSource(view.Name, view.Source.Clone(), recorder)
	})
	if err != nil {
		return nil, err
	}

	signature, err := readergraph.Signature(input, output, lookup)
	if err != nil {
		return nil, err
	}
	if signature != document.ABI {
		return nil, fmt.Errorf("reader metadata current contract closure differs from authored ABI")
	}
	checked, err := readergraph.AdmitWithLookup(component, input, output, assets, lookup)
	if err != nil {
		return nil, err
	}
	checked.SelectorCompanion, err = s.selectorAssociation(types)
	if err != nil {
		return nil, err
	}
	if checked.SelectorCompanion != nil {
		if _, err = checkedSelectorContract(checked, input, output, types); err != nil {
			return nil, err
		}
	}
	checked.ReaderGraph = checked.ReaderGraph.WithSelector(checked.SelectorCompanion)
	if err = readergraph.ValidateWithLookup(checked, input, output, store, lookup); err != nil {
		return nil, err
	}
	return checked, nil
}
func findReaderRow(typ reflect.Type, name string, seen map[reflect.Type]bool) *x.Type {
	for typ != nil && (typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
		typ = typ.Elem()
	}
	if typ == nil || seen[typ] {
		return nil
	}
	seen[typ] = true
	if typ.Name() != "" && typ.PkgPath()+"."+typ.Name() == name {
		return x.NewType(typ)
	}
	if typ.Kind() == reflect.Struct {
		for _, field := range reflect.VisibleFields(typ) {
			if field.IsExported() {
				if d := findReaderRow(field.Type, name, seen); d != nil {
					return d
				}
			}
		}
	}
	return nil
}

type readerMetadataResources struct {
	base      fs.FS
	namespace string
	assets    map[string][]byte
}

func (r readerMetadataResources) Open(uri string) (fs.File, error) {
	ns, p, ok := strings.Cut(uri, ":")
	if !ok || ns != r.namespace || !fs.ValidPath(p) {
		return nil, fmt.Errorf("reader metadata resource escapes selected holder authority")
	}
	body, err := fs.ReadFile(r.base, uri)
	if err != nil {
		return nil, err
	}
	r.assets[uri] = body
	return r.base.Open(uri)
}
