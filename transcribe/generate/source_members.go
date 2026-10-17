package generate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/tag"
	"github.com/viant/tagly/tags"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"go/format"
	"go/token"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

// SourceMemberFile is the existing build-selected source parse, shared by
// declaration admission and staged generation. Source seals its current preimage.
type SourceMemberFile struct {
	Source        string
	File          *ast.File
	CanonicalType func(string) (string, error) `json:"-" yaml:"-"`
}

func sourceStructs(file *ast.File) (map[string]*ast.StructType, error) {
	result := map[string]*ast.StructType{}
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, entry := range group.Specs {
			typ, ok := entry.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structure, ok := typ.Type.(*ast.StructType)
			if !ok || typ.Assign.IsValid() {
				continue
			}
			if result[typ.Name.Name] != nil {
				return nil, fmt.Errorf("duplicate struct declaration %s", typ.Name.Name)
			}
			result[typ.Name.Name] = structure
		}
	}
	return result, nil
}
func sourceFieldSignature(field *ast.Field) string {
	var b bytes.Buffer
	_ = format.Node(&b, token.NewFileSet(), field.Type)
	tag := ""
	if field.Tag != nil {
		tag = field.Tag.Value
	}
	return b.String() + "\x00" + tag
}

// retainSourceMemberAST edits a copy of only the targeted fresh declarations.
// The single build-selected authored parse supplies exact fields and ordering;
// fresh generated AST supplies all remaining members. No source is reparsed.
func retainSourceMemberAST(original, current *ast.File, selections []spec.SourceMember, initial bool, signatures ...func(*ast.File, *ast.Field) (string, error)) (*ast.File, error) {
	if original.Name.Name != current.Name.Name {
		return nil, fmt.Errorf("source member package differs from generated authority")
	}
	signature := func(file *ast.File, field *ast.Field) (string, error) { return sourceFieldSignature(field), nil }
	if len(signatures) > 0 && signatures[0] != nil {
		signature = signatures[0]
	}
	names := map[string]bool{}
	for _, declaration := range current.Decls {
		for _, name := range declarationNames(declaration) {
			names[name] = true
		}
	}
	for _, group := range original.Comments {
		for _, comment := range group.List {
			if strings.HasPrefix(comment.Text, "//go:build") || strings.HasPrefix(comment.Text, "// +build") || strings.HasPrefix(comment.Text, "//go:embed") {
				return nil, fmt.Errorf("source member destination contains unsupported file directive %s", comment.Text)
			}
		}
	}
	for _, declaration := range original.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok == token.IMPORT {
			continue
		}
		for _, name := range declarationNames(group) {
			if !names[name] {
				return nil, fmt.Errorf("source member destination contains unsupported application declaration %s", name)
			}
		}
		if group.Tok != token.TYPE {
			return nil, fmt.Errorf("source member destination contains unsupported application declaration")
		}
	}
	old, err := sourceStructs(original)
	if err != nil {
		return nil, err
	}
	fresh, err := sourceStructs(current)
	if err != nil {
		return nil, err
	}
	selected := map[string]map[string]bool{}
	for _, item := range selections {
		if selected[item.Owner] == nil {
			selected[item.Owner] = map[string]bool{}
		}
		if selected[item.Owner][item.Name] {
			return nil, fmt.Errorf("duplicate source member selection %s.%s", item.Owner, item.Name)
		}
		selected[item.Owner][item.Name] = true
	}
	replacements := map[string]*ast.StructType{}
	for owner, names := range selected {
		if old[owner] == nil || fresh[owner] == nil {
			return nil, fmt.Errorf("source member owner %s requires local concrete structs", owner)
		}
		fields := map[string]*ast.Field{}
		fieldSignatures := map[string]string{}
		preserved := map[string]*ast.Field{}
		var expected, actual []string
		for _, field := range fresh[owner].Fields.List {
			if len(field.Names) != 1 {
				return nil, fmt.Errorf("generated owner %s requires direct fields", owner)
			}
			name := field.Names[0].Name
			if names[name] {
				return nil, fmt.Errorf("source member %s.%s has generated ownership", owner, name)
			}
			fields[name] = field
			sig, err := signature(current, field)
			if err != nil {
				return nil, err
			}
			fieldSignatures[name] = sig
			expected = append(expected, name+"\x00"+sig)
		}
		if len(fields) == 0 {
			return nil, fmt.Errorf("source member owner %s has no independently generated members", owner)
		}
		retained := map[string][]*ast.Field{}
		seen := map[string]bool{}
		anchor := ""
		for _, field := range old[owner].Fields.List {
			if len(field.Names) != 1 {
				return nil, fmt.Errorf("source member owner %s requires unique direct fields", owner)
			}
			name := field.Names[0].Name
			if seen[name] {
				return nil, fmt.Errorf("duplicate source member %s.%s", owner, name)
			}
			seen[name] = true
			if names[name] {
				tag := ""
				if field.Tag != nil {
					tag, _ = strconv.Unquote(field.Tag.Value)
				}
				if name == "Has" || reflect.StructTag(tag).Get("setMarker") != "" {
					return nil, fmt.Errorf("source members cannot own presence markers")
				}
				retained[anchor] = append(retained[anchor], field)
				continue
			}
			sig, err := signature(original, field)
			if err != nil {
				return nil, err
			}
			actual = append(actual, name+"\x00"+sig)
			if fieldSignatures[name] == sig {
				preserved[name] = field
			}
			if fields[name] != nil {
				anchor = name
			}
		}
		for name := range names {
			if !seen[name] {
				return nil, fmt.Errorf("stale source member selection %s.%s", owner, name)
			}
		}
		if initial && !reflect.DeepEqual(actual, expected) {
			for i := 0; i < len(actual) || i < len(expected); i++ {
				var have, want string
				if i < len(actual) {
					have = actual[i]
				}
				if i < len(expected) {
					want = expected[i]
				}
				if have != want {
					return nil, fmt.Errorf("source member owner %s differs from independently generated authority at field %d: source=%q generated=%q", owner, i+1, have, want)
				}
			}
			return nil, fmt.Errorf("source member owner %s differs from independently generated authority", owner)
		}
		ordered := append([]*ast.Field(nil), retained[""]...)
		for _, field := range fresh[owner].Fields.List {
			chosen := field
			if prior := preserved[field.Names[0].Name]; prior != nil {
				chosen = prior
			}
			ordered = append(ordered, chosen)
			ordered = append(ordered, retained[field.Names[0].Name]...)
		}
		structure := *fresh[owner]
		list := *structure.Fields
		list.List = ordered
		structure.Fields = &list
		replacements[owner] = &structure
	}
	// Copy only AST nodes that change; shared compilation syntax is immutable.
	result := *current
	result.Decls = append([]ast.Decl(nil), current.Decls...)
	for i, declaration := range current.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		copied := *group
		copied.Specs = append([]ast.Spec(nil), group.Specs...)
		for j, entry := range group.Specs {
			typ, ok := entry.(*ast.TypeSpec)
			if !ok || replacements[typ.Name.Name] == nil {
				continue
			}
			copyType := *typ
			copyType.Type = replacements[typ.Name.Name]
			copied.Specs[j] = &copyType
		}
		result.Decls[i] = &copied
	}
	return &result, nil
}

// Existing native type/tag parsers compare semantics; raw source fields remain
// untouched when their complete independently generated contract agrees.
func sourceFieldSemanticSignature(file *ast.File, field *ast.Field, pkg string, normalize func(string) (string, error)) (string, error) {
	imports := map[string]string{}
	var fileImports []*ast.ImportSpec
	for _, decl := range file.Decls {
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.IMPORT {
			for _, entry := range group.Specs {
				fileImports = append(fileImports, entry.(*ast.ImportSpec))
			}
		}
	}
	for _, item := range fileImports {
		p, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			return "", err
		}
		alias := path.Base(p)
		if item.Name != nil {
			alias = item.Name.Name
		}
		if prior := imports[alias]; prior != "" && prior != p {
			return "", fmt.Errorf("source member import alias %s ambiguous", alias)
		}
		imports[alias] = p
	}
	identity, err := (xshape.Resolver{Package: pkg, Imports: imports}).Canonical(field.Type)
	if err != nil {
		return "", err
	}
	if normalize != nil {
		identity, err = normalize(identity)
		if err != nil {
			return "", err
		}
	}
	raw := ""
	if field.Tag != nil {
		raw, err = strconv.Unquote(field.Tag.Value)
		if err != nil {
			return "", err
		}
	}
	name := field.Names[0].Name
	parsed, err := tag.ParseField(reflect.StructField{Name: name, Tag: reflect.StructTag(raw)})
	if err != nil {
		return "", err
	}
	if parsed.Binding != nil {
		if parsed.Binding.DataType == "" {
			parsed.Binding.DataType = identity
		} else {
			provider, err := (xshape.Resolver{Package: pkg, Imports: imports}).Canonical(parsed.Binding.DataType)
			if err != nil {
				return "", err
			}
			if normalize != nil {
				provider, err = normalize(provider)
				if err != nil {
					return "", err
				}
			}
			parsed.Binding.DataType = provider
		}
		// Bindly's existing IsRequired contract treats absent and false alike.
		required := parsed.Binding.Required != nil && *parsed.Binding.Required
		parsed.Binding.Required = &required
	}
	decoded := map[string]string{}
	parsedTags, err := tags.Parse(raw)
	if err != nil {
		return "", err
	}
	for _, item := range parsedTags {
		if _, exists := decoded[item.Name]; exists {
			return "", fmt.Errorf("source member %s has duplicate tag %s", name, item.Name)
		}
		value, ok := reflect.StructTag(raw).Lookup(item.Name)
		if !ok {
			return "", fmt.Errorf("source member %s has malformed tag %s", name, item.Name)
		}
		if item.Name == "parameter" || item.Name == "bind" {
			first, options := tags.Values(value).Name()
			if strings.Contains(first, "=") {
				options = tags.Values(value)
			}
			seenOptions := map[string]bool{}
			if err := options.MatchPairs(func(key, value string) error {
				if seenOptions[key] {
					return fmt.Errorf("source member %s has duplicate binding option %s", name, key)
				}
				seenOptions[key] = true
				return nil
			}); err != nil {
				return "", err
			}
		}
		decoded[item.Name] = value
	}
	delete(decoded, "parameter")
	delete(decoded, "bind")
	encoded, err := json.Marshal(struct {
		Type    string
		Tags    map[string]string
		Binding any
	}{identity, decoded, parsed.Binding})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
