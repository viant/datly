// Package presencegen generates sparse comparisons against persisted Previous values.
package presencegen

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"

	sqlxio "github.com/viant/sqlx/io"
)

type Config struct {
	SourceDir string
	Entry     string
	Output    string
}

type sourceType struct {
	name   string
	fields []sourceField
}

type sourceField struct {
	name    string
	typeAST ast.Expr
	typeGo  string
	tag     string
	imports map[string]string
}

type entity struct {
	rowOnly   bool
	typeDef   *sourceType
	marker    *sourceType
	preserved []sourceField
	scalars   []sourceField
	relations []relation
	identity  []sourceField
}

type relation struct {
	field      sourceField
	child      *entity
	collection bool
	elementPtr bool
	pointer    bool
}

type model struct {
	packageName string
	entry       *entity
	entities    []*entity
	omit        map[string]bool
}

type packageSource struct {
	name  string
	types map[string]*sourceType
}

func Run(cfg Config) error {
	if cfg.Output == "" {
		cfg.Output = filepath.Join(cfg.SourceDir, lowerSnake(cfg.Entry)+"_presence.go")
	}
	source, err := generate(cfg.SourceDir, cfg.Entry, cfg.Output)
	if err != nil {
		return err
	}
	if err = os.WriteFile(cfg.Output, source, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", cfg.Output, err)
	}
	return nil
}

func RunRecursive(root string, output io.Writer) error {
	var patchDirs []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || filepath.Base(path) != "patch" {
			return nil
		}
		patchDirs = append(patchDirs, path)
		return filepath.SkipDir
	})
	if err != nil {
		return fmt.Errorf("walk %s: %w", root, err)
	}
	sort.Strings(patchDirs)
	generatedCount := 0
	for _, dir := range patchDirs {
		entries, discoverErr := discoverEntries(dir)
		if discoverErr != nil {
			return discoverErr
		}
		if len(entries) == 0 {
			fmt.Fprintf(output, "skip %s: no request-body entity with an existing Has marker\n", dir)
			continue
		}
		emitted := map[string]bool{}
		for _, entry := range entries {
			dest := filepath.Join(dir, lowerSnake(entry)+"_presence.go")
			source, generateErr := generateExcluding(dir, entry, dest, emitted)
			if generateErr != nil {
				return fmt.Errorf("generate %s entry %s: %w", dir, entry, generateErr)
			}
			if writeErr := os.WriteFile(dest, source, 0o644); writeErr != nil {
				return fmt.Errorf("write %s: %w", dest, writeErr)
			}
			for _, receiver := range generatedReceivers(source) {
				emitted[receiver] = true
			}
			generatedCount++
			fmt.Fprintf(output, "generated %s (%s)\n", dest, entry)
		}
	}
	fmt.Fprintf(output, "generated %d presence file(s) from %d patch package(s)\n", generatedCount, len(patchDirs))
	return nil
}

func discoverEntries(sourceDir string) ([]string, error) {
	parsed, err := parseSource(sourceDir, "")
	if err != nil {
		return nil, err
	}
	input := parsed.types["Input"]
	if input == nil {
		return nil, nil
	}
	entries := map[string]bool{}
	visited := map[string]bool{}
	var findMarkers func(string)
	findMarkers = func(typeName string) {
		if typeName == "" || visited[typeName] {
			return
		}
		visited[typeName] = true
		typeDef := parsed.types[typeName]
		if typeDef == nil {
			return
		}
		if markerTypeName(typeDef) != "" {
			entries[typeName] = true
			return
		}
		for _, field := range typeDef.fields {
			childName, _, _, _ := relationType(field.typeAST)
			findMarkers(childName)
		}
	}
	for _, field := range input.fields {
		if !parameterOption(field.tag, "kind", "body") {
			continue
		}
		entryName, _, _, _ := relationType(field.typeAST)
		findMarkers(entryName)
	}
	result := make([]string, 0, len(entries))
	for entry := range entries {
		result = append(result, entry)
	}
	sort.Strings(result)
	return result, nil
}

func parseSource(sourceDir, output string) (*packageSource, error) {
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, sourceDir, func(info os.FileInfo) bool {
		name := info.Name()
		if strings.HasSuffix(name, "_test.go") || strings.HasSuffix(name, "_presence.go") {
			return false
		}
		if output != "" && filepath.Clean(filepath.Join(sourceDir, name)) == filepath.Clean(output) {
			return false
		}
		return strings.HasSuffix(name, ".go")
	}, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", sourceDir, err)
	}
	if len(packages) != 1 {
		return nil, fmt.Errorf("expected one Go package in %s, found %d", sourceDir, len(packages))
	}
	var pkg *ast.Package
	for _, candidate := range packages {
		pkg = candidate
	}
	types := map[string]*sourceType{}
	for _, file := range pkg.Files {
		imports := fileImports(file)
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.TYPE {
				continue
			}
			for _, specification := range general.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				typeDef := &sourceType{name: typeSpec.Name.Name}
				for _, field := range structType.Fields.List {
					for _, name := range field.Names {
						typeDef.fields = append(typeDef.fields, sourceField{
							name: name.Name, typeAST: field.Type, typeGo: printNode(fset, field.Type),
							tag: fieldTag(field), imports: imports,
						})
					}
				}
				types[typeDef.name] = typeDef
			}
		}
	}
	return &packageSource{name: pkg.Name, types: types}, nil
}

func generate(sourceDir, entryName, output string) ([]byte, error) {
	return generateExcluding(sourceDir, entryName, output, nil)
}

func generateExcluding(sourceDir, entryName, output string, omit map[string]bool) ([]byte, error) {
	parsed, err := parseSource(sourceDir, output)
	if err != nil {
		return nil, err
	}
	return generateTypes(parsed.name, parsed.types, []string{entryName}, omit, Options{})
}

// Options selects row-local generation for the universal writer, which owns
// graph identity, child matching and projection provenance independently.
type Options struct{ RowOnly bool }

// Generate renders comparisons from planned Go sources without writing or
// reparsing application files. Entries must belong to this concrete package.
func Generate(packageName string, sources map[string][]byte, entries []string, options Options) ([]byte, error) {
	fset := token.NewFileSet()
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	var files []*ast.File
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, sources[name], parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		if file.Name.Name != packageName {
			return nil, fmt.Errorf("source %s belongs to package %s, expected %s", name, file.Name.Name, packageName)
		}
		files = append(files, file)
	}
	types := map[string]*sourceType{}
	for _, file := range files {
		imports := fileImports(file)
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.TYPE {
				continue
			}
			for _, specification := range general.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				typeDef := &sourceType{name: typeSpec.Name.Name}
				for _, field := range structType.Fields.List {
					if len(field.Names) == 0 {
						continue
					}
					for _, name := range field.Names {
						typeDef.fields = append(typeDef.fields, sourceField{
							name:    name.Name,
							typeAST: field.Type,
							typeGo:  printNode(fset, field.Type),
							tag:     fieldTag(field),
							imports: imports,
						})
					}
				}
				types[typeDef.name] = typeDef
			}
		}
	}

	return generateTypes(packageName, types, entries, nil, options)
}

func generateTypes(packageName string, types map[string]*sourceType, entries []string, omit map[string]bool, options Options) ([]byte, error) {
	built := map[string]*entity{}
	result := &model{packageName: packageName, omit: omit}
	var build func(*sourceType) (*entity, error)
	build = func(typeDef *sourceType) (*entity, error) {
		if found := built[typeDef.name]; found != nil {
			return found, nil
		}
		markerName := markerTypeName(typeDef)
		marker := types[markerName]
		if marker == nil {
			return nil, nil
		}
		current := &entity{typeDef: typeDef, marker: marker, identity: identityFields(typeDef), rowOnly: options.RowOnly}
		preserved := primaryPresenceFields(typeDef)
		if options.RowOnly {
			// Native writers carry authoritative SQLX primary-key metadata. Do
			// not classify a writable field as identity from its Go name.
			current.identity, preserved = nil, nil
			for _, field := range typeDef.fields {
				if tagOption(field.tag, "primaryKey") {
					current.identity = append(current.identity, field)
					preserved = append(preserved, field)
				}
			}
		}
		built[typeDef.name] = current
		result.entities = append(result.entities, current)
		markerFields := make(map[string]bool, len(marker.fields))
		for _, field := range marker.fields {
			markerFields[field.name] = true
		}
		for _, field := range preserved {
			if markerFields[field.name] {
				current.preserved = append(current.preserved, field)
			}
		}
		for _, field := range typeDef.fields {
			role := strings.ToLower(strings.TrimSpace(structTag(field.tag, "writer")))
			if markerFields[field.name] && (role == "delete" || role == "concurrency") && !identityField(current.preserved, field.name) {
				current.preserved = append(current.preserved, field)
			}
		}
		for _, field := range typeDef.fields {
			if field.name == "Has" || !markerFields[field.name] {
				continue
			}
			childName, collection, elementPtr, pointer := relationType(field.typeAST)
			if childType := types[childName]; childType != nil && !options.RowOnly {
				child, buildErr := build(childType)
				if buildErr != nil {
					return nil, buildErr
				}
				if child != nil {
					current.relations = append(current.relations, relation{field: field, child: child, collection: collection, elementPtr: elementPtr, pointer: pointer})
					continue
				}
			}
			if options.RowOnly && (types[childName] != nil && markerTypeName(types[childName]) != "" || structTag(field.tag, "view") != "" || structTag(field.tag, "on") != "") {
				continue
			}
			if transient(field.tag) || identityField(current.identity, field.name) || identityField(current.preserved, field.name) {
				continue
			}
			if !comparableType(field.typeAST) {
				return nil, fmt.Errorf("%s.%s has unsupported marked type %s", typeDef.name, field.name, field.typeGo)
			}
			current.scalars = append(current.scalars, field)
		}
		return current, nil
	}
	for _, entryName := range entries {
		entryType := types[entryName]
		if entryType == nil {
			return nil, fmt.Errorf("entry entity %s was not found", entryName)
		}
		entry, err := build(entryType)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, fmt.Errorf("entry entity %s has no existing setMarker Has field", entryName)
		}
	}

	for _, parent := range result.entities {
		for _, rel := range parent.relations {
			if rel.collection && len(rel.child.identity) == 0 {
				return nil, fmt.Errorf("%s.%s child %s has no primary or unique identity", parent.typeDef.name, rel.field.name, rel.child.typeDef.name)
			}
		}
	}
	return result.render()
}

func (m *model) render() ([]byte, error) {
	var out strings.Builder
	out.WriteString("// Code generated by presencegen. DO NOT EDIT.\n\n")
	fmt.Fprintf(&out, "package %s\n", m.packageName)
	imports := make([]string, 0, 1)
	needsCompare := false
	neededImports := map[string]string{}
	for _, item := range m.entities {
		if m.omit[item.typeDef.name] {
			continue
		}
		for _, field := range item.scalars {
			if sliceType(field.typeAST) {
				needsCompare = true
			}
		}
		for _, rel := range item.relations {
			for _, identity := range rel.child.identity {
				collectTypeImports(neededImports, identity)
			}
		}
	}
	if needsCompare {
		imports = append(imports, "presenceSlices \"slices\"")
	}
	for alias, path := range neededImports {
		if alias == filepath.Base(path) {
			imports = append(imports, strconv.Quote(path))
		} else {
			imports = append(imports, alias+" "+strconv.Quote(path))
		}
	}
	sort.Strings(imports)
	if len(imports) > 0 {
		out.WriteString("\nimport (\n")
		for _, item := range imports {
			fmt.Fprintf(&out, "\t%s\n", item)
		}
		out.WriteString(")\n")
	}
	out.WriteString("\n")
	for _, item := range m.entities {
		if !item.rowOnly && !m.omit[item.typeDef.name] && len(item.identity) > 1 {
			renderIdentityType(&out, item)
		}
	}
	for _, item := range m.entities {
		if m.omit[item.typeDef.name] {
			continue
		}
		renderMethod(&out, item)
	}
	for _, parent := range m.entities {
		if m.omit[parent.typeDef.name] {
			continue
		}
		for _, rel := range parent.relations {
			if rel.collection {
				renderCollection(&out, parent, rel)
			}
		}
	}
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return nil, fmt.Errorf("format generated source: %w\n%s", err, out.String())
	}
	return formatted, nil
}

func generatedReceivers(source []byte) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "generated.go", source, 0)
	if err != nil {
		return nil
	}
	var result []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "SynchronizePresentMarker" || function.Recv == nil || len(function.Recv.List) != 1 {
			continue
		}
		receiver := function.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		if ident, ok := receiver.(*ast.Ident); ok {
			result = append(result, ident.Name)
		}
	}
	return result
}

func renderMethod(out *strings.Builder, item *entity) {
	receiver := receiverName(item.typeDef.name)
	fmt.Fprintf(out, "// SynchronizePresentMarker removes unchanged fields from the existing Has marker.\n")
	fmt.Fprintf(out, "// Fields not already marked present are not compared. A nil snapshot is a no-op.\n")
	fmt.Fprintf(out, "func (%s *%s) SynchronizePresentMarker(prev *%s) {\n", receiver, item.typeDef.name, item.typeDef.name)
	fmt.Fprintf(out, "\tif %s == nil || prev == nil || %s.Has == nil {\n\t\treturn\n\t}\n\n", receiver, receiver)
	fmt.Fprintf(out, "\tpresent := *%s.Has\n", receiver)
	if item.rowOnly {
		out.WriteString("\thas := &present\n")
	} else {
		fmt.Fprintf(out, "\thas := &%s{}\n", item.marker.name)
	}
	fmt.Fprintf(out, "\t%s.Has = nil\n", receiver)
	for _, field := range item.preserved {
		fmt.Fprintf(out, "\thas.%s = present.%s\n", field.name, field.name)
	}
	for _, field := range item.scalars {
		if _, ok := field.typeAST.(*ast.StarExpr); ok {
			fmt.Fprintf(out, "\thas.%s = present.%s && !(%s.%s == prev.%s || (%s.%s != nil && prev.%s != nil && *%s.%s == *prev.%s))\n", field.name, field.name, receiver, field.name, field.name, receiver, field.name, field.name, receiver, field.name, field.name)
		} else if sliceType(field.typeAST) {
			fmt.Fprintf(out, "\thas.%s = present.%s && !((%s.%s == nil) == (prev.%s == nil) && presenceSlices.Equal(%s.%s, prev.%s))\n", field.name, field.name, receiver, field.name, field.name, receiver, field.name, field.name)
		} else {
			fmt.Fprintf(out, "\thas.%s = present.%s && %s.%s != prev.%s\n", field.name, field.name, receiver, field.name, field.name)
		}
	}
	for _, rel := range item.relations {
		if rel.collection {
			fmt.Fprintf(out, "\thas.%s = present.%s && %s(%s.%s, prev.%s)\n", rel.field.name, rel.field.name, collectionHelper(item, rel), receiver, rel.field.name, rel.field.name)
			continue
		}
		fmt.Fprintf(out, "\tif present.%s {\n", rel.field.name)
		if rel.pointer {
			fmt.Fprintf(out, "\t\tswitch {\n\t\tcase %s.%s == nil && prev.%s != nil:\n\t\t\thas.%s = true\n", receiver, rel.field.name, rel.field.name, rel.field.name)
			fmt.Fprintf(out, "\t\tcase %s.%s != nil && prev.%s == nil:\n\t\t\thas.%s = true\n", receiver, rel.field.name, rel.field.name, rel.field.name)
			fmt.Fprintf(out, "\t\tcase %s.%s != nil:\n\t\t\t%s.%s.SynchronizePresentMarker(prev.%s)\n\t\t\thas.%s = %s\n\t\t}\n", receiver, rel.field.name, receiver, rel.field.name, rel.field.name, rel.field.name, markerChangedExpr(receiver+"."+rel.field.name, rel.child))
		} else {
			fmt.Fprintf(out, "\t\t%s.%s.SynchronizePresentMarker(&prev.%s)\n\t\thas.%s = %s\n", receiver, rel.field.name, rel.field.name, rel.field.name, markerChangedExpr(receiver+"."+rel.field.name, rel.child))
		}
		out.WriteString("\t}\n")
	}
	fmt.Fprintf(out, "\n\tif *has != (%s{}) {\n\t\t%s.Has = has\n\t}\n}\n\n", item.marker.name, receiver)
}

func renderCollection(out *strings.Builder, parent *entity, rel relation) {
	child := rel.child
	keyType := identityType(child)
	fmt.Fprintf(out, "func %s(current, previous %s) bool {\n", collectionHelper(parent, rel), rel.field.typeGo)
	fmt.Fprintf(out, "\tpreviousByIdentity := make(map[%s]*%s, len(previous))\n", keyType, child.typeDef.name)
	fmt.Fprintf(out, "\tpreviousCount := make(map[%s]int, len(previous))\n", keyType)
	out.WriteString("\tchanged := (current == nil) != (previous == nil) || len(current) != len(previous)\n")
	out.WriteString("\tfor index := range previous {\n")
	renderItem(out, "previous", rel.elementPtr)
	renderIdentity(out, child)
	out.WriteString("\t\tif !identified {\n\t\t\tchanged = true\n\t\t\tcontinue\n\t\t}\n\t\tpreviousCount[identity]++\n\t\tpreviousByIdentity[identity] = item\n\t}\n")
	fmt.Fprintf(out, "\tcurrentCount := make(map[%s]int, len(current))\n", keyType)
	out.WriteString("\tfor index := range current {\n")
	renderItem(out, "current", rel.elementPtr)
	renderIdentity(out, child)
	out.WriteString("\t\tif identified {\n\t\t\tcurrentCount[identity]++\n\t\t}\n\t}\n")
	out.WriteString("\tfor _, count := range previousCount {\n\t\tif count != 1 {\n\t\t\tchanged = true\n\t\t}\n\t}\n")
	out.WriteString("\tfor index := range current {\n")
	renderItem(out, "current", rel.elementPtr)
	renderIdentity(out, child)
	out.WriteString("\t\tif !identified || currentCount[identity] != 1 || previousCount[identity] != 1 {\n\t\t\tchanged = true\n\t\t\tcontinue\n\t\t}\n")
	out.WriteString("\t\tpreviousItem, matched := previousByIdentity[identity]\n\t\tif !matched {\n\t\t\tchanged = true\n\t\t\tcontinue\n\t\t}\n")
	fmt.Fprintf(out, "\t\titem.SynchronizePresentMarker(previousItem)\n\t\tif %s {\n\t\t\tchanged = true\n\t\t}\n\t\tdelete(previousByIdentity, identity)\n\t}\n", markerChangedExpr("item", child))
	out.WriteString("\tif len(previousByIdentity) != 0 {\n\t\tchanged = true\n\t}\n\treturn changed\n}\n\n")
}

func renderItem(out *strings.Builder, collection string, pointer bool) {
	if pointer {
		fmt.Fprintf(out, "\t\titem := %s[index]\n\t\tif item == nil {\n\t\t\tchanged = true\n\t\t\tcontinue\n\t\t}\n", collection)
		return
	}
	fmt.Fprintf(out, "\t\titem := &%s[index]\n", collection)
}

func renderIdentity(out *strings.Builder, item *entity) {
	if len(item.identity) == 1 {
		field := item.identity[0]
		if pointer, ok := field.typeAST.(*ast.StarExpr); ok {
			fmt.Fprintf(out, "\t\tidentified := item.%s != nil\n\t\tvar identity %s\n\t\tif identified {\n\t\t\tidentity = *item.%s\n\t\t}\n", field.name, printExpr(pointer.X), field.name)
			return
		}
		fmt.Fprintf(out, "\t\tidentity := item.%s\n\t\tvar zeroIdentity %s\n\t\tidentified := identity != zeroIdentity\n", field.name, field.typeGo)
		return
	}
	fmt.Fprintf(out, "\t\tidentity := %s{", identityType(item))
	for index, field := range item.identity {
		if index > 0 {
			out.WriteString(", ")
		}
		fmt.Fprintf(out, "%s: item.%s", field.name, field.name)
	}
	fmt.Fprintf(out, "}\n\t\tidentified := identity != (%s{})\n", identityType(item))
}

func renderIdentityType(out *strings.Builder, item *entity) {
	fmt.Fprintf(out, "type %s struct {\n", identityType(item))
	for _, field := range item.identity {
		fmt.Fprintf(out, "\t%s %s\n", field.name, field.typeGo)
	}
	out.WriteString("}\n\n")
}

func identityType(item *entity) string {
	if len(item.identity) == 1 {
		if pointer, ok := item.identity[0].typeAST.(*ast.StarExpr); ok {
			return printExpr(pointer.X)
		}
		return item.identity[0].typeGo
	}
	return lowerFirst(item.typeDef.name) + "PresenceIdentity"
}

func collectionHelper(parent *entity, rel relation) string {
	return lowerFirst(parent.typeDef.name) + rel.field.name + "PresenceChanged"
}

func markerTypeName(typeDef *sourceType) string {
	for _, field := range typeDef.fields {
		if field.name != "Has" || !tagOption(field.tag, "setMarker") {
			continue
		}
		if pointer, ok := field.typeAST.(*ast.StarExpr); ok {
			if ident, ok := pointer.X.(*ast.Ident); ok {
				return ident.Name
			}
		}
	}
	return ""
}

func identityFields(typeDef *sourceType) []sourceField {
	var primary, unique []sourceField
	for _, field := range typeDef.fields {
		if tagOption(field.tag, "primaryKey") {
			primary = append(primary, field)
		}
		if tagOption(field.tag, "unique") {
			unique = append(unique, field)
		}
	}
	if len(primary) > 0 {
		return primary
	}
	if len(unique) > 0 {
		return unique
	}
	for _, field := range typeDef.fields {
		if field.name == "Id" || field.name == "ID" {
			return []sourceField{field}
		}
	}
	var idFields []sourceField
	for _, field := range typeDef.fields {
		if strings.HasSuffix(field.name, "Id") || strings.HasSuffix(field.name, "ID") {
			idFields = append(idFields, field)
		}
	}
	return idFields
}

func primaryPresenceFields(typeDef *sourceType) []sourceField {
	var primary []sourceField
	for _, field := range typeDef.fields {
		if tagOption(field.tag, "primaryKey") {
			primary = append(primary, field)
		}
	}
	if len(primary) > 0 {
		return primary
	}
	for _, field := range typeDef.fields {
		if field.name == "Id" || field.name == "ID" {
			return []sourceField{field}
		}
	}
	return nil
}

func markerChangedExpr(receiver string, item *entity) string {
	fields := make([]string, 0, len(item.scalars)+len(item.relations))
	for _, field := range item.scalars {
		fields = append(fields, receiver+".Has."+field.name)
	}
	for _, relation := range item.relations {
		fields = append(fields, receiver+".Has."+relation.field.name)
	}
	if len(fields) == 0 {
		return "false"
	}
	return receiver + ".Has != nil && (" + strings.Join(fields, " || ") + ")"
}

func identityField(identity []sourceField, name string) bool {
	for _, field := range identity {
		if field.name == name {
			return true
		}
	}
	return false
}

func sliceType(expr ast.Expr) bool {
	array, ok := expr.(*ast.ArrayType)
	return ok && array.Len == nil
}

func relationType(expr ast.Expr) (name string, collection, elementPtr, pointer bool) {
	switch actual := expr.(type) {
	case *ast.StarExpr:
		name, collection, elementPtr, _ = relationType(actual.X)
		return name, collection, elementPtr, true
	case *ast.ArrayType:
		if actual.Len != nil {
			return "", false, false, false
		}
		if ptr, ok := actual.Elt.(*ast.StarExpr); ok {
			if ident, ok := ptr.X.(*ast.Ident); ok {
				return ident.Name, true, true, false
			}
		}
		if ident, ok := actual.Elt.(*ast.Ident); ok {
			return ident.Name, true, false, false
		}
	case *ast.Ident:
		return actual.Name, false, false, false
	}
	return "", false, false, false
}

func comparableType(expr ast.Expr) bool {
	switch actual := expr.(type) {
	case *ast.Ident, *ast.SelectorExpr, *ast.StarExpr, *ast.ChanType:
		return true
	case *ast.ArrayType:
		return actual.Len != nil && comparableType(actual.Elt) || actual.Len == nil && primitiveType(actual.Elt)
	default:
		return false
	}
}

func primitiveType(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	switch ident.Name {
	case "bool", "string",
		"int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
		"byte", "rune", "float32", "float64", "complex64", "complex128":
		return true
	default:
		return false
	}
}

func transient(tag string) bool {
	return sqlxio.ParseTag(reflect.StructTag(tag)).Transient
}

func tagOption(tag, option string) bool {
	if option == "setMarker" {
		return structTag(tag, option) == "true"
	}
	parsed := sqlxio.ParseTag(reflect.StructTag(tag))
	switch option {
	case "primaryKey":
		return parsed.PrimaryKey
	case "unique":
		return parsed.IsUnique
	}
	for _, item := range strings.Split(structTag(tag, "sqlx"), ",") {
		if strings.EqualFold(strings.TrimSpace(item), option) {
			return true
		}
	}
	return false
}

func parameterOption(tag, key, expected string) bool {
	for _, item := range strings.Split(structTag(tag, "parameter"), ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], key) && strings.EqualFold(parts[1], expected) {
			return true
		}
	}
	return false
}

func structTag(tag, key string) string {
	for tag != "" {
		tag = strings.TrimLeft(tag, " ")
		index := strings.IndexByte(tag, ':')
		if index <= 0 || index+1 >= len(tag) || tag[index+1] != '"' {
			return ""
		}
		name := tag[:index]
		value, err := strconv.Unquote(tag[index+1:])
		if err == nil {
			if name == key {
				return value
			}
			return ""
		}
		end := index + 2
		for end < len(tag) {
			if tag[end] == '"' && tag[end-1] != '\\' {
				break
			}
			end++
		}
		if end >= len(tag) {
			return ""
		}
		if name == key {
			value, _ = strconv.Unquote(tag[index+1 : end+1])
			return value
		}
		tag = tag[end+1:]
	}
	return ""
}

func fieldTag(field *ast.Field) string {
	if field.Tag == nil {
		return ""
	}
	value, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return ""
	}
	return value
}

func fileImports(file *ast.File) map[string]string {
	result := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		alias := filepath.Base(path)
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		result[alias] = path
	}
	return result
}

func collectTypeImports(target map[string]string, field sourceField) {
	ast.Inspect(field.typeAST, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if ident, ok := selector.X.(*ast.Ident); ok {
			if path := field.imports[ident.Name]; path != "" {
				target[ident.Name] = path
			}
		}
		return true
	})
}

func printNode(fset *token.FileSet, node ast.Node) string {
	var buffer bytes.Buffer
	_ = printer.Fprint(&buffer, fset, node)
	return buffer.String()
}

func printExpr(expr ast.Expr) string {
	return printNode(token.NewFileSet(), expr)
}

func receiverName(name string) string {
	for _, char := range name {
		return strings.ToLower(string(char))
	}
	return "e"
}

func lowerFirst(value string) string {
	if value == "" {
		return value
	}
	runes := []rune(value)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func lowerSnake(value string) string {
	var out strings.Builder
	for index, char := range value {
		if unicode.IsUpper(char) && index > 0 {
			out.WriteByte('_')
		}
		out.WriteRune(unicode.ToLower(char))
	}
	return out.String()
}
