package generate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/x"
)

// BorrowedLeafContract is detached comparison evidence, never an execution plan.
type BorrowedLeafContract struct {
	// Physical contains only copied native values, never a live schema capability.
	Physical json.RawMessage
	// Keep native serialization policy even when current nullable fields happen
	// to produce identical tags; schema evolution must not erase that authority.
	WriterOmitEmpty bool
	Package         string
	Name            string
	Connector       string
	Catalog         string
	Schema          string
	Table           string
	Columns         []*spec.Column
	Fields          []Field
	MarkerFields    []Field
	Metadata        *spec.View
	Wrapper         string
}

// BorrowedAuthorityFile seals bytes, not size/mtime. Bytes are excluded from the
// published provenance receipt; the receipt identifies the protected content.
type BorrowedAuthorityFile struct {
	Path   string
	Mode   os.FileMode
	SHA256 string
	Bytes  []byte `json:"-"`
}

type BorrowedRowAuthority struct {
	Declaration    spec.BorrowedSQLRow
	BorrowerSource string
	BorrowerScope  string
	BorrowerName   string
	BorrowerGraph  []string
	OwnerSource    string
	OwnerScope     string
	OwnerName      string
	OwnerOperation string
	OwnerGraph     []string
	OwnerDirectory string
	RowFile        string
	HasFile        string
	Helpers        []BorrowedHelper
	Expected       BorrowedLeafContract
	Files          []BorrowedAuthorityFile
	// Revalidation queries fresh schema using the same immutable authored inputs.
	// This callback has no write or runtime component capability.
	ValidateSchema func() error          `json:"-"`
	ReceiptGuard   *BorrowedReceiptGuard `json:"-"`
}

func BorrowedLeafContractFor(component *spec.Component, view *spec.View, packagePath, name, connector, catalog, schema, table string) (BorrowedLeafContract, error) {
	if component == nil || view == nil || view.Source == nil || len(view.Relations) != 0 || view.SelfReference != nil {
		return BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row requires a SQL-derived leaf")
	}
	if connector == "" || schema == "" || table != view.Source.Table || table == "" {
		return BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row physical connector/schema/table origin is unknown or conflicts with canonical leaf")
	}

	for _, col := range view.Columns {
		if col == nil || col.NameInferred || col.Type.Name == "" || col.Type.Name == "any" || col.Expression != "" {
			return BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row requires fresh SQL-derived direct columns")
		}
	}
	copy := component.Clone()
	copy.RootView = view.Clone()
	copy.RootView.TypeName = name
	copy.RootView.Dest = ""
	copy.Views = nil
	copy.Parameters = nil
	copy.Name = "BorrowedLeafComparison"
	if copy.Settings == nil {
		copy.Settings = &spec.Settings{}
	} else {
		copy.Settings = copy.Settings.Clone()
	}
	// Preserve native shaping policy; only the explicit borrowing identity list
	// is irrelevant to a detached independently generated scalar expectation.
	if copy.Settings.Generation != nil {
		copy.Settings.Generation.BorrowedSQLRows = nil
	}
	copy.Settings.Mutation = "post"
	copy.RootView.EntityHooks = ""
	id, err := copy.RootView.Identity()
	if err != nil {
		return BorrowedLeafContract{}, err
	}
	plan, err := New(Input{Component: copy, TargetPackage: packagePath, SetMarkerViews: map[string]bool{id: true}}).Plan()
	if err != nil {
		return BorrowedLeafContract{}, err
	}
	var row *ViewPlan
	for i := range plan.Views {
		if plan.Views[i].Identity == id {
			row = &plan.Views[i]
		}
	}
	if row == nil {
		return BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row comparison row not generated")
	}
	fields, err := canonicalBorrowedFields(row.Fields, plan.Imports, packagePath)
	if err != nil {
		return BorrowedLeafContract{}, err
	}
	markers := make([]Field, len(row.SetMarkerFields))
	for i, name := range row.SetMarkerFields {
		markers[i] = Field{Name: name, Type: "bool"}
	}
	metadata := view.Clone()
	// Graph names and resource spelling identify the two independent occurrences;
	// their origins are retained separately. Execution and serialization facts
	// survive; documentation provenance may differ between query occurrences.
	metadata.Key = spec.Key{}
	metadata.Name, metadata.Namespace, metadata.TypeName, metadata.Dest = "", "", "", ""
	metadata.Auxiliary, metadata.QueueContract = false, ""
	metadata.Reconciliation = nil
	metadata.Columns = nil
	metadata.Source = &spec.ViewSource{Table: table, Controls: view.Source.Controls.Clone(), Bindings: view.Source.Bindings.Clone()}
	if metadata.Source.Bindings == nil {
		metadata.Source.Bindings = &spec.ViewBindings{}
	}
	metadata.Source.Bindings.Connector = connector
	result := BorrowedLeafContract{WriterOmitEmpty: copy.Settings.Generation != nil && copy.Settings.Generation.WriterOmitEmpty, Metadata: metadata, Package: packagePath, Name: name, Connector: connector, Catalog: catalog, Schema: schema, Table: table, Fields: fields, MarkerFields: markers}
	for _, column := range view.Columns {
		cloned := column.Clone()
		cloned.Tag = dtag.CanonicalFieldTag(cloned.Tag)
		result.Columns = append(result.Columns, cloned)
	}
	return result, nil
}

// BorrowedBodyWrapper uses native generated contract fields to retain the exact
// terminal pointer/container expression at a resolved body holder path.
func BorrowedBodyWrapper(component *spec.Component, bodyPath, packagePath string) (string, error) {
	plan, err := New(Input{Component: component, TargetPackage: packagePath}).Plan()
	if err != nil {
		return "", err
	}
	parts := strings.Split(bodyPath, "/")
	fields := plan.Input.Fields
	var expression string
	for i, part := range parts {
		found := false
		for _, field := range fields {
			if field.Name == part {
				expression = field.Type
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("borrow_sql_row body holder %s is absent from native input contract", part)
		}
		if i < len(parts)-1 {
			base := strings.TrimLeft(expression, "[]*")
			view := plan.ViewByType(base)
			if view == nil {
				return "", fmt.Errorf("borrow_sql_row body holder has a non-SQL-generated wrapper %s", expression)
			}
			fields = view.Fields
		}
	}
	canonical, err := canonicalBorrowedFields([]Field{{Type: expression}}, plan.Imports, packagePath)
	if err != nil {
		return "", err
	}
	return canonical[0].Type, nil
}

func canonicalBorrowedFields(fields []Field, imports []spec.ImportSpec, packagePath string) ([]Field, error) {
	names := map[string]string{}
	for _, imp := range imports {
		alias := imp.Alias
		if alias == "" {
			alias = filepath.Base(imp.Package)
		}
		names[alias] = imp.Package
	}
	result := append([]Field(nil), fields...)
	for i := range result {
		expr, err := parser.ParseExpr(result[i].Type)
		if err != nil {
			return nil, err
		}
		typ, err := canonicalType(expr, names, packagePath)
		if err != nil {
			return nil, err
		}
		result[i].Type = typ
		result[i].Tag = dtag.CanonicalFieldTag(result[i].Tag)
	}
	return result, nil
}

func CompareBorrowedLeafContracts(owner, borrower BorrowedLeafContract) error {
	// All compared facts are retained. Mutation role/queue policy never enter
	// this detached contract and remain on the original component views.
	if !reflect.DeepEqual(owner, borrower) {
		return fmt.Errorf("borrow_sql_row independently derived owner and borrower leaf contracts differ")
	}
	return nil
}

// RetainBorrowedSeal never adopts a second read as a replacement baseline.
func RetainBorrowedSeal(a *BorrowedRowAuthority, file BorrowedAuthorityFile) error {
	for _, existing := range a.Files {
		if existing.Path == file.Path {
			if existing.Mode != file.Mode || !bytes.Equal(existing.Bytes, file.Bytes) {
				return fmt.Errorf("borrow_sql_row dependency changed between validated artifact and closure sealing: %s", file.Path)
			}
			return nil
		}
	}
	a.Files = append(a.Files, file)
	return nil
}

func SealBorrowedAuthorityFile(path string) (BorrowedAuthorityFile, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return BorrowedAuthorityFile{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return BorrowedAuthorityFile{}, err
	}
	if !info.Mode().IsRegular() {
		return BorrowedAuthorityFile{}, fmt.Errorf("borrow_sql_row dependency %q is not a regular file", absolute)
	}
	data, err := os.ReadFile(absolute)
	if err != nil {
		return BorrowedAuthorityFile{}, err
	}
	sum := sha256.Sum256(data)
	return BorrowedAuthorityFile{Path: absolute, Mode: info.Mode(), SHA256: hex.EncodeToString(sum[:]), Bytes: data}, nil
}

func (a *BorrowedRowAuthority) Clone() *BorrowedRowAuthority {
	if a == nil {
		return nil
	}
	copy := *a
	if a.ReceiptGuard != nil {
		guard := *a.ReceiptGuard
		guard.Expected = append([]byte(nil), guard.Expected...)
		guard.Prior.Bytes = append([]byte(nil), guard.Prior.Bytes...)
		copy.ReceiptGuard = &guard
	}
	copy.BorrowerGraph = append([]string(nil), a.BorrowerGraph...)
	copy.OwnerGraph = append([]string(nil), a.OwnerGraph...)
	copy.Expected.Metadata = a.Expected.Metadata.Clone()
	copy.Expected.Physical = append(json.RawMessage(nil), a.Expected.Physical...)
	copy.Helpers = append([]BorrowedHelper(nil), a.Helpers...)
	copy.Expected.Fields = append([]Field(nil), a.Expected.Fields...)
	copy.Expected.MarkerFields = append([]Field(nil), a.Expected.MarkerFields...)
	copy.Expected.Columns = nil
	for _, c := range a.Expected.Columns {
		copy.Expected.Columns = append(copy.Expected.Columns, c.Clone())
	}
	copy.Files = append([]BorrowedAuthorityFile(nil), a.Files...)
	for i := range copy.Files {
		copy.Files[i].Bytes = append([]byte(nil), a.Files[i].Bytes...)
	}
	return &copy
}

// ValidateBorrowedDescriptor compares independently generated expectations with
// the descriptor's actual declaration, including complete marker membership.
func ValidateBorrowedDescriptor(contract BorrowedLeafContract, row, marker *x.Type) error {
	for i, item := range []*x.Type{row, marker} {
		name := contract.Name
		fields := contract.Fields
		if i == 1 {
			name += "Has"
			fields = contract.MarkerFields
		}
		if item == nil || item.Name != name || item.PkgPath != contract.Package || item.SynteticType == nil || item.SynteticType.TypeSpec == nil {
			return fmt.Errorf("borrow_sql_row %s requires an actual package source descriptor", name)
		}
		imports := map[string]string{}
		for alias, imp := range item.SynteticType.Imports {
			if imp != nil {
				imports[alias] = imp.Path
			}
		}
		if err := validateBorrowedStruct(item.SynteticType.TypeSpec, imports, contract.Package, fields); err != nil {
			return err
		}
	}
	return nil
}

func validateBorrowedStruct(ts *ast.TypeSpec, imports map[string]string, pkg string, want []Field) error {
	structure, ok := ts.Type.(*ast.StructType)
	if !ok || ts.Assign.IsValid() || ts.TypeParams != nil {
		return fmt.Errorf("borrow_sql_row declaration %s must own an ordinary struct", ts.Name)
	}
	var actual []Field
	for _, field := range structure.Fields.List {
		if len(field.Names) != 1 {
			return fmt.Errorf("borrow_sql_row declaration %s has anonymous/grouped fields", ts.Name)
		}
		typ, err := canonicalType(field.Type, imports, pkg)
		if err != nil {
			return err
		}
		tag := ""
		if field.Tag != nil {
			tag, err = strconv.Unquote(field.Tag.Value)
			if err != nil {
				return err
			}
		}
		actual = append(actual, Field{Name: field.Names[0].Name, Type: typ, Tag: dtag.CanonicalFieldTag(tag)})
	}
	if len(actual) != len(want) {
		return fmt.Errorf("borrow_sql_row declaration %s has stale field membership", ts.Name)
	}
	for i, f := range actual {
		if f.Name != want[i].Name || f.Type != want[i].Type || f.Tag != dtag.CanonicalFieldTag(want[i].Tag) {
			return fmt.Errorf("borrow_sql_row declaration %s field %d %s differs from fresh SQL contract", ts.Name, i, f.Name)
		}
	}
	return nil
}

// BorrowedNativeOwnerArtifacts derives the native owner view/route/resource
// declarations without staging or publication. Their bytes must already exist.
func BorrowedNativeOwnerArtifacts(input Input, dir string) ([]BorrowedAuthorityFile, error) {
	plan, err := New(input).Plan()
	if err != nil {
		return nil, err
	}
	files, _, _, err := scaffoldArtifacts(dir, plan)
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{filepath.Join(dir, plan.ViewDest): true, filepath.Join(dir, plan.RouterDest): true}
	for _, view := range plan.Views {
		if view.Ownership == ViewGenerated {
			selected[filepath.Join(dir, view.Destination)] = true
		}
	}
	if plan.Resources != nil {
		selected[filepath.Join(dir, plan.Resources.Destination)] = true
		for _, file := range plan.Resources.Files {
			selected[filepath.Join(dir, file.Path)] = true
		}
	}
	var result []BorrowedAuthorityFile
	for _, file := range files {
		if !selected[file.Path] {
			continue
		}
		sealed, err := SealBorrowedAuthorityFile(file.Path)
		if err != nil {
			return nil, fmt.Errorf("borrow_sql_row native owner metadata/resource missing: %w", err)
		}
		if string(sealed.Bytes) != file.Content {
			return nil, fmt.Errorf("borrow_sql_row native owner metadata/resource is stale: %s", file.Path)
		}
		result = append(result, sealed)
	}
	return result, nil
}

// BorrowedHelper retains a freshly generated row method and its exact owner.
type BorrowedHelper struct{ Receiver, Name, Source, Path string }

// BorrowedNativeHelpers selects canonical row methods from an existing native
// generator asset. It does not generate or publish a component.
func BorrowedNativeHelpers(asset *EntitySupportAsset, row string) ([]BorrowedHelper, error) {
	if asset == nil || asset.File == nil {
		return nil, fmt.Errorf("borrow_sql_row native owner helper contract is missing")
	}
	var result []BorrowedHelper
	for _, decl := range asset.File.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok || method.Recv == nil || len(method.Recv.List) != 1 {
			continue
		}
		typ := method.Recv.List[0].Type
		if ptr, ok := typ.(*ast.StarExpr); ok {
			typ = ptr.X
		}
		receiver, ok := typ.(*ast.Ident)
		if !ok || receiver.Name != row {
			continue
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), method); err != nil {
			return nil, err
		}
		result = append(result, BorrowedHelper{Receiver: row, Name: method.Name.Name, Source: out.String()})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("borrow_sql_row native owner row helpers are missing")
	}
	return result, nil
}

func (a *BorrowedRowAuthority) validateHelpers(resolve func(string) string) error {
	for _, helper := range a.Helpers {
		if helper.Path == "" {
			return fmt.Errorf("borrow_sql_row required helper %s.%s is missing", helper.Receiver, helper.Name)
		}
		content, err := os.ReadFile(resolve(helper.Path))
		if err != nil {
			return err
		}
		if generatedOwner(content) != a.OwnerName {
			return fmt.Errorf("borrow_sql_row helper ownership differs")
		}
		file, err := parser.ParseFile(token.NewFileSet(), helper.Path, content, parser.ParseComments)
		if err != nil {
			return err
		}
		count := 0
		for _, decl := range file.Decls {
			method, ok := decl.(*ast.FuncDecl)
			if !ok || method.Recv == nil || len(method.Recv.List) != 1 || method.Name.Name != helper.Name {
				continue
			}
			typ := method.Recv.List[0].Type
			if ptr, ok := typ.(*ast.StarExpr); ok {
				typ = ptr.X
			}
			receiver, ok := typ.(*ast.Ident)
			if !ok || receiver.Name != helper.Receiver {
				continue
			}
			count++
			var out bytes.Buffer
			if err = format.Node(&out, token.NewFileSet(), method); err != nil {
				return err
			}
			if out.String() != helper.Source {
				return fmt.Errorf("borrow_sql_row helper %s.%s differs from native owner contract", helper.Receiver, helper.Name)
			}
		}
		if count != 1 {
			return fmt.Errorf("borrow_sql_row required helper %s.%s missing/ambiguous", helper.Receiver, helper.Name)
		}
	}
	return nil
}

// ValidateFiles validates protected content at the caller's projected/protected
// path mapping. It unconditionally reads all dependencies, ignoring mtime.
func (a *BorrowedRowAuthority) ValidateFiles(resolve func(string) string) error {
	if a == nil {
		return fmt.Errorf("borrow_sql_row authority missing")
	}
	for _, sealed := range a.Files {
		path := resolve(sealed.Path)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode() != sealed.Mode || !info.Mode().IsRegular() {
			return fmt.Errorf("borrow_sql_row dependency mode/type drift: %s", sealed.Path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, sealed.Bytes) {
			return fmt.Errorf("borrow_sql_row dependency content drift: %s", sealed.Path)
		}
	}
	for i, file := range []string{a.RowFile, a.HasFile} {
		data, err := os.ReadFile(resolve(file))
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, data, parser.ParseComments)
		if err != nil {
			return err
		}
		if generatedOwner(data) != a.OwnerName {
			return fmt.Errorf("borrow_sql_row declaration has a foreign/generated-owner mismatch: %s", file)
		}
		imports := map[string]string{}
		for _, imp := range parsed.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			alias := filepath.Base(p)
			if imp.Name != nil {
				alias = imp.Name.Name
			}
			imports[alias] = p
		}
		name := a.Expected.Name
		want := a.Expected.Fields
		if i == 1 {
			name += "Has"
			want = a.Expected.MarkerFields
		}
		count := 0
		for _, decl := range parsed.Decls {
			g, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, s := range g.Specs {
				ts, ok := s.(*ast.TypeSpec)
				if ok && ts.Name.Name == name {
					count++
					if err := validateBorrowedStruct(ts, imports, a.Expected.Package, want); err != nil {
						return err
					}
				}
			}
		}
		if count != 1 {
			return fmt.Errorf("borrow_sql_row declaration %s missing or ambiguous", name)
		}
	}
	if err := a.validateHelpers(resolve); err != nil {
		return err
	}
	if a.ValidateSchema != nil {
		return a.ValidateSchema()
	}
	return nil
}

func borrowedIdentityPath(path string, root string, replacement string) string {
	if withinScaffoldTree(root, path) {
		rel, _ := filepath.Rel(root, path)
		return filepath.Join(replacement, rel)
	}
	return path
}

func (s *packageSet) validateBorrowedRows() error {
	for i, p := range s.plans {
		for _, a := range p.BorrowedRows {
			if filepath.Clean(a.OwnerDirectory) != filepath.Clean(s.dirs[i]) {
				return fmt.Errorf("borrow_sql_row owner and borrower must publish in the same package forest")
			}
			if err := a.ValidateProjectedFiles(func(path string) string {
				for _, f := range s.forests {
					if withinScaffoldTree(f.target, path) {
						return borrowedIdentityPath(path, f.target, f.stage)
					}
				}
				return path
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *packageSet) protectBorrowedRows() {
	for _, f := range s.forests {
		for _, p := range s.plans {
			for _, a := range p.BorrowedRows {
				if withinScaffoldTree(f.target, a.OwnerDirectory) {
					f.borrowed = append(f.borrowed, a.Clone())
				}
			}
		}
	}
}

// BorrowedReceiptGuard protects the first observed receipt or its absence.
// It is never read from a published receipt as borrowing authority.
type BorrowedReceiptGuard struct {
	Path         string
	Exists       bool
	Prior        BorrowedAuthorityFile
	Expected     []byte
	ExpectedMode os.FileMode
}

func (g *BorrowedReceiptGuard) ValidatePrior(resolve func(string) string) error {
	if g == nil {
		return nil
	}
	path := resolve(g.Path)
	info, err := os.Lstat(path)
	if !g.Exists {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("borrow_sql_row receipt appeared after ownership observation: %s", g.Path)
	}
	if err != nil {
		return fmt.Errorf("borrow_sql_row receipt disappeared: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode() != g.Prior.Mode {
		return fmt.Errorf("borrow_sql_row receipt mode/type drift")
	}
	bytesNow, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(bytesNow, g.Prior.Bytes) {
		return fmt.Errorf("borrow_sql_row receipt content drift")
	}
	return nil
}
func (a *BorrowedRowAuthority) ValidateProjectedFiles(resolve func(string) string) error {
	if a == nil {
		return fmt.Errorf("borrow_sql_row authority missing")
	}
	if err := a.ReceiptGuard.ValidatePrior(func(path string) string { return path }); err != nil {
		return err
	}
	return a.validatePreparedFiles(resolve)
}

// validatePreparedFiles rereads the staged authority closure and expected receipt.
// The protected original receipt is validated separately at its backup mapping
// during publication; a prepared receipt never becomes its ownership baseline.
func (a *BorrowedRowAuthority) validatePreparedFiles(resolve func(string) string) error {
	if err := a.ValidateFiles(resolve); err != nil {
		return err
	}
	g := a.ReceiptGuard
	if g == nil {
		return nil
	}
	path := resolve(g.Path)
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode() != g.ExpectedMode {
		return fmt.Errorf("borrow_sql_row prepared receipt mode/type drift")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, g.Expected) {
		return fmt.Errorf("borrow_sql_row prepared receipt content drift")
	}
	return nil
}
func (a *BorrowedRowAuthority) ValidateProtectedFiles(resolve func(string) string) error {
	if err := a.ValidateFiles(resolve); err != nil {
		return err
	}
	return a.ReceiptGuard.ValidatePrior(resolve)
}

func borrowedReceiptFiles(dir string, plan *Plan) ([]EmittedFile, error) {
	if len(plan.BorrowedRows) == 0 {
		return nil, nil
	}
	path := filepath.Join(dir, plan.Generation.File("borrowed_authority", "borrowed_authority.json"))
	guard := plan.BorrowedRows[0].ReceiptGuard
	if guard != nil {
		if guard.Path != path {
			return nil, fmt.Errorf("borrow_sql_row receipt destination changed during preparation")
		}
		if err := guard.ValidatePrior(func(path string) string { return path }); err != nil {
			return nil, err
		}
		current, err := json.MarshalIndent(plan.BorrowedRows, "", "  ")
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(append(current, '\n'), guard.Expected) {
			return nil, fmt.Errorf("borrow_sql_row receipt authority changed during preparation")
		}
		return []EmittedFile{{Path: path, Content: string(guard.Expected)}}, nil
	}
	guard = &BorrowedReceiptGuard{Path: path, ExpectedMode: 0644}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("borrow_sql_row receipt destination is not a regular file")
		}
		sealed, err := SealBorrowedAuthorityFile(path)
		if err != nil {
			return nil, err
		}
		guard.Exists = true
		guard.Prior = sealed
		guard.ExpectedMode = sealed.Mode
		existing := sealed.Bytes
		var previous []*BorrowedRowAuthority
		if err = json.Unmarshal(existing, &previous); err != nil || len(previous) == 0 {
			return nil, fmt.Errorf("borrow_sql_row receipt destination has foreign content")
		}
		for _, old := range previous {
			if old == nil || old.BorrowerSource != plan.BorrowedRows[0].BorrowerSource || old.BorrowerScope != plan.BorrowedRows[0].BorrowerScope || old.BorrowerName != plan.BorrowedRows[0].BorrowerName {
				return nil, fmt.Errorf("borrow_sql_row receipt destination has foreign ownership")
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	data, err := json.MarshalIndent(plan.BorrowedRows, "", "  ")
	if err != nil {
		return nil, err
	}
	guard.Expected = append(data, '\n')
	for _, authority := range plan.BorrowedRows {
		authority.ReceiptGuard = guard
	}
	return []EmittedFile{{Path: path, Content: string(guard.Expected)}}, nil
}
