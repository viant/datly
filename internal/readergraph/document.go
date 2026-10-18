// Package readergraph transports canonical reader metadata in the existing spec
// representation. It never interprets DQL or grants generated-file ownership.
package readergraph

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/readergraph/model"
	"github.com/viant/datly/internal/typeidentity"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"io"
	"io/fs"
	"reflect"
	"strings"
)

type Document struct {
	ABI       string            `json:"abi"`
	Version   int               `json:"version"`
	Package   string            `json:"package"`
	Holder    string            `json:"holder"`
	Field     string            `json:"field"`
	Input     string            `json:"input"`
	Output    string            `json:"output"`
	Rows      map[string]string `json:"rows"`
	Component *spec.Component   `json:"component"`
}

func Bytes(c *spec.Component) ([]byte, error) { return json.Marshal(c) }

// StrictDecode checks the JSON tree before spec custom unmarshallers run. A
// round trip then rejects keys/aliases silently ignored by nested unmarshallers.
func StrictDecode(body []byte) (*Document, error) {
	if len(body) > 16<<20 {
		return nil, fmt.Errorf("reader metadata exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := strictValue(decoder); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("reader metadata trailing data")
	}
	var d Document
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&d); err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	var before, after any
	first := json.NewDecoder(bytes.NewReader(body))
	first.UseNumber()
	second := json.NewDecoder(bytes.NewReader(normalized))
	second.UseNumber()
	if err = first.Decode(&before); err != nil {
		return nil, err
	}
	if err = second.Decode(&after); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before, after) {
		return nil, fmt.Errorf("reader metadata unknown, aliased or unrepresentable JSON field")
	}
	if len(d.ABI) != 64 {
		return nil, fmt.Errorf("reader metadata ABI fingerprint is required")
	}
	if _, err := hex.DecodeString(d.ABI); err != nil {
		return nil, err
	}
	if d.Version != 1 || d.Component == nil || d.Package == "" || d.Holder == "" || d.Field == "" || d.Input == "" || d.Output == "" {
		return nil, fmt.Errorf("incomplete reader metadata identity")
	}
	return &d, nil
}
func strictValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return fmt.Errorf("invalid metadata key")
			}
			fold := strings.ToLower(name)
			if seen[fold] {
				return fmt.Errorf("duplicate or aliased reader metadata key %s", name)
			}
			seen[fold] = true
			if err = strictValue(d); err != nil {
				return err
			}
		}
	} else if delim == '[' {
		for d.More() {
			if err = strictValue(d); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("invalid metadata delimiter")
	}
	_, err = d.Token()
	return err
}
func Contracts(c *spec.Component, input, output *x.Type, lookup xshape.Lookup) error {
	if c == nil || input == nil || output == nil {
		return fmt.Errorf("reader metadata exact contracts required")
	}
	if len(c.Views) != 0 {
		return fmt.Errorf("reader metadata independent input views are unsupported")
	}
	if input.Name == "" || output.Name == "" || input.PkgPath != c.Key.Scope || output.PkgPath != c.Key.Scope {
		return fmt.Errorf("reader metadata requires same-package nominal contracts")
	}
	if len(c.Routes) != 1 || c.Routes[0] == nil || c.Routes[0].Handler != "" || c.Routes[0].Method != "GET" || c.RootView == nil {
		return fmt.Errorf("reader metadata supports one ordinary GET reader")
	}
	if c.Settings == nil || c.Settings.Mutation != "" || c.Settings.SequenceStrategy != "" || c.Settings.ComponentCallPolicy != "" || c.Settings.IndependentChildTransactions || c.Settings.WarmupTarget != nil || len(c.Settings.ProtectedFlushTables) > 0 {
		return fmt.Errorf("reader metadata unsupported execution owner")
	}
	rootSeen := 0
	independent := map[*spec.View]bool{}
	for _, p := range spec.EffectiveParameters(c.Parameters) {
		if p == nil {
			continue
		}
		shape := input
		if p.Source.Kind == "output" || p.EmitOutput {
			shape = output
		}
		fields, err := xshape.New(shape, lookup).Fields()
		if err != nil {
			return err
		}
		var field *xshape.Field
		for i := range fields {
			if strings.EqualFold(fields[i].Name, p.Name) {
				if field != nil {
					return fmt.Errorf("ambiguous reader parameter %s", p.Name)
				}
				field = &fields[i]
			}
		}
		if field == nil {
			if p.QuerySelector != nil {
				continue
			}
			return fmt.Errorf("reader metadata parameter %s has no contract field", p.Name)
		}

		metadata, err := dtag.ParseField(field.StructField())
		if err != nil {
			return err
		}
		if metadata.Binding == nil || !strings.EqualFold(metadata.Binding.Location.Kind, p.Source.Kind) || !strings.EqualFold(metadata.Binding.Location.In, p.Source.Name) {
			return fmt.Errorf("reader metadata parameter %s has incompatible binding metadata", p.Name)
		}
		if p.Source.Kind == "view" || (p.Source.Kind == "output" && p.Source.Name == "view") {
			var view *spec.View
			if p.Source.Kind == "output" {
				rootSeen++
				view = c.RootView
			} else {
				for _, v := range c.Views {
					if v.Name == p.Source.Name || v.Key.Name == p.Source.Name {
						if view != nil {
							return fmt.Errorf("ambiguous reader metadata view binding")
						}
						view = v
					}
				}
			}
			if view == nil {
				return fmt.Errorf("reader metadata view binding is missing")
			}
			if p.Source.Kind == "view" {
				independent[view] = true
			}
			selected, err := xshape.New(shape, lookup).ResolveField(field.Name)
			if err != nil {
				return err
			}
			if selected.Descriptor == nil {
				return fmt.Errorf("reader metadata view has no exact row")
			}
			imports := map[string]string{}
			if c.TypeContext != nil {
				for _, i := range c.TypeContext.Imports {
					imports[i.Alias] = i.Package
				}
			}
			canonical, err := (xshape.Resolver{Package: shape.PkgPath, Imports: imports}).Canonical(view.TypeName)
			if err != nil {
				return err
			}
			if canonical != selected.Descriptor.Key() {
				return fmt.Errorf("reader metadata view row differs from exact contract field")
			}
		}
		want := p.OutputTypeExpr
		if want == "" && p.Codec == nil {
			want = p.TypeExpr
		}
		if want != "" && want != "?" {
			resolved, err := xshape.New(shape, lookup).ResolveField(field.Name)
			if err != nil {
				return err
			}
			imports := map[string]string{}
			if c.TypeContext != nil {
				for _, i := range c.TypeContext.Imports {
					imports[i.Alias] = i.Package
				}
			}
			canonical, err := (xshape.Resolver{Package: shape.PkgPath, Imports: imports}).Canonical(want)
			if err != nil {
				return err
			}
			if canonical != resolved.Identity {
				// Source aliases and reflected fields may spell the same Go type
				// differently; only true aliases can reconcile their identities.
				if lookup != nil {
					declaredIdentity, err := typeidentity.Alias(canonical, lookup, "reader metadata")
					if err != nil {
						return err
					}
					linkedIdentity, err := typeidentity.Alias(resolved.Identity, lookup, "reader metadata")
					if err != nil {
						return err
					}
					if declaredIdentity == linkedIdentity {
						continue
					}
				}
				return fmt.Errorf("reader metadata field %s type %s differs from %s", field.Name, canonical, resolved.Identity)
			}
		}
	}
	if rootSeen != 1 || len(independent) != len(c.Views) {
		return fmt.Errorf("reader metadata requires exact root and independent view bindings")
	}
	return nil
}
func Validate(c *spec.Component, input, output *x.Type, resources fs.FS) error {
	return ValidateWithLookup(c, input, output, resources, nil)
}
func ValidateWithLookup(c *spec.Component, input, output *x.Type, resources fs.FS, lookup xshape.Lookup) error {
	if c == nil || c.ReaderGraph == nil {
		return nil
	}
	body, err := Bytes(c)
	if err != nil {
		return err
	}
	if input == nil || output == nil {
		return fmt.Errorf("reader metadata exact contracts required")
	}
	if err = c.ReaderGraph.ValidateSelector(c.SelectorCompanion); err != nil {
		return err
	}
	signature, err := Signature(input, output, lookup)
	if err != nil {
		return err
	}
	if err = c.ReaderGraph.ValidateContracts(signature); err != nil {
		return err
	}
	return c.ReaderGraph.Validate(body, input.Key(), output.Key(), resources)
}
func Admit(c *spec.Component, input, output *x.Type, assets map[string][]byte) (*spec.Component, error) {
	return AdmitWithLookup(c, input, output, assets, nil)
}
func AdmitWithLookup(c *spec.Component, input, output *x.Type, assets map[string][]byte, lookup xshape.Lookup) (*spec.Component, error) {
	result := c.Clone()
	body, err := Bytes(result)
	if err != nil {
		return nil, err
	}
	signature, err := Signature(input, output, lookup)
	if err != nil {
		return nil, err
	}
	result.ReaderGraph = model.New(body, input.Key(), output.Key(), assets, signature)
	return result, nil
}

func WalkViews(c *spec.Component, visit func(*spec.View) error) error {
	seen := map[*spec.View]bool{}
	active := map[*spec.View]bool{}
	var walk func(*spec.View) error
	walk = func(v *spec.View) error {
		if v == nil {
			return nil
		}
		if active[v] {
			return fmt.Errorf("reader metadata graph cycle")
		}
		if seen[v] {
			return nil
		}
		seen[v] = true
		active[v] = true
		defer delete(active, v)
		if err := visit(v); err != nil {
			return err
		}
		for _, r := range v.Relations {
			if r == nil || r.View == nil {
				return fmt.Errorf("incomplete reader metadata relation")
			}
			if err := walk(r.View); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(c.RootView); err != nil {
		return err
	}
	for _, v := range c.Views {
		if err := walk(v); err != nil {
			return err
		}
	}
	return nil
}
func CheckRows(d *Document, lookup xshape.Lookup) error {
	seen := map[string]bool{}
	err := WalkViews(d.Component, func(v *spec.View) error {
		identity, err := v.Identity()
		if err != nil {
			return err
		}
		wanted, ok := d.Rows[identity]
		if !ok || wanted != v.TypeName {
			return fmt.Errorf("reader metadata missing exact row identity %s", identity)
		}
		seen[identity] = true
		row, err := lookup(wanted)
		if err != nil {
			return err
		}
		if row == nil || row.Name == "" || row.PkgPath != d.Package {
			return fmt.Errorf("reader metadata row %s lacks same-package authority", wanted)
		}
		shape := xshape.New(row, lookup)
		if len(v.Columns) == 0 {
			return fmt.Errorf("reader metadata has unsupported absent projection at %s", identity)
		}
		for _, column := range v.Columns {
			if column == nil {
				return fmt.Errorf("nil metadata column")
			}
			field, err := resolveRowColumn(shape, column)
			if err != nil {
				return err
			}
			if field.Descriptor == nil && field.Identity == "" {
				return fmt.Errorf("metadata column %s has no Go field", column.Name)
			}
			typ := column.EffectiveType()
			expression := typ.Name
			if typ.Package != "" {
				expression = typ.Package + "." + expression
			}
			if typ.Pointer {
				expression = "*" + expression
			}
			if typ.Cardinality == spec.CardinalityMany {
				if typ.SlicePointer {
					expression = "*" + expression
				}
				expression = "[]" + expression
			}
			canonical, err := (xshape.Resolver{Package: row.PkgPath}).Canonical(expression)
			if err != nil {
				return err
			}
			if canonical != field.Identity {
				return fmt.Errorf("reader metadata column %s type %s differs from %s", column.Name, canonical, field.Identity)
			}
		}
		for _, relation := range v.Relations {
			if relation.Kind == spec.RelationKindDerived {
				continue
			}
			field, err := shape.ResolveField(relation.Holder)
			if err != nil {
				return err
			}
			if field.Descriptor == nil || field.Descriptor.Key() != relation.View.TypeName {
				return fmt.Errorf("reader metadata relation %s row identity mismatch", relation.Holder)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(seen) != len(d.Rows) {
		return fmt.Errorf("reader metadata contains foreign row identity")
	}
	return nil
}

// Signature freezes the real contract closure, including pointer/collection
// shape, anonymous embeddings, presence markers and authored wire/binding tags.
func Signature(input, output *x.Type, lookup xshape.Lookup) (string, error) {
	type field struct {
		Name      string
		Type      string
		Anonymous bool
		Index     []int
		Tag       string
	}
	signatures := map[string][]field{}
	active := map[string]bool{}
	var visit func(*x.Type) error
	visit = func(descriptor *x.Type) error {
		if descriptor == nil {
			return fmt.Errorf("reader metadata missing contract descriptor")
		}
		key := descriptor.Key()
		if active[key] {
			return nil
		}
		active[key] = true
		shape := xshape.New(descriptor, lookup)
		isStruct, err := shape.IsStruct()
		if err != nil {
			return err
		}
		if !isStruct {
			return nil
		}
		fields, err := shape.Fields()
		if err != nil {
			return err
		}
		var result []field
		for _, f := range fields {
			if !f.Exported {
				continue
			}
			identity, err := f.CanonicalType()
			if err != nil {
				return err
			}
			result = append(result, field{Name: f.Name, Type: identity, Anonymous: f.Anonymous, Index: append([]int(nil), f.Index...), Tag: string(f.Tag)})
			resolved, err := shape.ResolveField(f.Name)
			if err != nil {
				return err
			}
			if resolved.Descriptor != nil {
				if err = visit(resolved.Descriptor); err != nil {
					return err
				}
			}
		}
		signatures[key] = result
		return nil
	}
	if err := visit(input); err != nil {
		return "", err
	}
	if err := visit(output); err != nil {
		return "", err
	}
	body, err := json.Marshal(signatures)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
