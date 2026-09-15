package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// EntityMethod records generated public methods for authored-source ownership checks.
type EntityMethod struct {
	Receiver, Name, ValueType string
	Getter                    bool
	Signature                 string
}
type EntityAsset struct {
	identity        *mutationIdentityPolicy
	File            *ast.File
	CaptureFunction string
	Methods         []EntityMethod
	SnapshotType    string
	SyncContextType string
	SyncMethod      string
	Associations    []EntityAssociation
	Invariants      []EntityInvariant
}

// EntityInvariant names generated backfill support for one canonical role/group.
type EntityInvariant struct {
	Identity                string
	Path                    plan.FieldPath
	Group, BackfillFunction string
}

type EntityAssociation struct {
	Identity string
	Path     plan.FieldPath
	Field    string
	// KeyAdapterType exposes original captured identity, never working values.
	StateType, KeyType, KeyAdapterType string
	CurrentKeyFunction                 string
}

// EntitySupport emits typed original-state and setter AST for both targets.
func EntitySupport(value *plan.Plan, config Config) (*EntityAsset, error) {
	lowered := &lowerer{plan: value, config: config}
	if err := lowered.prepare(); err != nil {
		return nil, err
	}
	return lowered.entitySupport()
}

type entityEmitter struct {
	identity                  *mutationIdentityPolicy
	l                         *lowerer
	prefix, snapshot, capture string
	asset                     *EntityAsset
	setters                   map[string]string
	shapeAlias                string
	fmtAlias                  string
	reflectAlias              string
}

func (l *lowerer) entitySupport() (*EntityAsset, error) {
	return (&entityEmitter{l: l}).emit()
}

func (e *entityEmitter) emit() (*EntityAsset, error) {
	l := e.l
	if l.plan.Root.Entity == nil || l.plan.Root.Auxiliary {
		return nil, nil
	}
	original := l.usedImports
	l.usedImports = map[string]bool{}
	for key, value := range original {
		l.usedImports[key] = value
	}
	defer func() { l.usedImports = original }()
	prefix := "_" + lowerInitial(l.factory)
	e.prefix, e.snapshot, e.capture = prefix, prefix+"OriginalInput", prefix+"CaptureInput"
	e.asset, e.setters = &EntityAsset{identity: e.identity}, map[string]string{}
	e.shapeAlias = l.importsByPath["github.com/viant/x/shape"]
	if e.shapeAlias == "" {
		e.shapeAlias = l.availableAlias("xshape")
		l.pathsByAlias[e.shapeAlias] = "github.com/viant/x/shape"
	}
	e.reflectAlias = l.importsByPath["reflect"]
	if e.reflectAlias == "" {
		e.reflectAlias = l.availableAlias("reflect")
		l.pathsByAlias[e.reflectAlias] = "reflect"
	}
	file := &ast.File{Name: ast.NewIdent(l.config.Package)}
	snapshotDeclaration, err := e.snapshotDeclaration()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, snapshotDeclaration)
	accessPlan, err := e.accessPlanDeclaration()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, accessPlan)
	file.Decls = append(file.Decls, e.syncContextDeclaration())
	file.Decls = append(file.Decls, e.markerDeclarations()...)
	for _, record := range l.records {
		if !e.enabled(record) {
			continue
		}
		if record.plan.Entity.Owned && !token.IsIdentifier(record.value.base) {
			return nil, fmt.Errorf("entity helpers require package-owned setter receivers, got %s", record.value.base)
		}
		declaration, err := e.originalDeclaration(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, declaration, e.availableDeclaration(record), e.hasDeclaration(record), e.presenceAssertion(record))
		setters, err := e.accessorDeclarations(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, setters...)
		invariants, err := e.invariantDeclarations(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, invariants...)
		capture, err := e.recordCaptureDeclaration(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, capture)
		attachment, err := e.attachDeclaration(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, attachment)
		synchronization, err := e.syncDeclarations(record)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, synchronization...)
	}
	capture, err := e.inputCaptureDeclaration()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, capture)
	if e.identity != nil {
		producers, err := e.identity.capture(e)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, producers)
	}
	file.Decls = append(file.Decls, e.markerAllocator())
	cloneOptions, err := e.cloneOptionsDeclaration()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, cloneOptions)
	syncInput, err := e.syncInputDeclarations()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, syncInput...)
	lookup, err := e.lookupDeclarations()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, lookup...)
	file.Decls = append([]ast.Decl{e.importDeclaration(file)}, file.Decls...)
	e.asset.File = file
	e.asset.CaptureFunction = e.capture
	e.asset.SnapshotType = e.snapshot
	e.asset.SyncContextType = e.syncName()
	e.asset.SyncMethod = "synchronize"
	for _, record := range l.records {
		if e.enabled(record) {
			e.asset.Associations = append(e.asset.Associations, EntityAssociation{Identity: record.plan.Identity, Path: append(plan.FieldPath(nil), record.plan.InputPath...), Field: "matched" + strconv.Itoa(record.order), StateType: e.stateName(record), KeyType: e.matchKeyName(record), KeyAdapterType: e.matchAdapterName(record), CurrentKeyFunction: e.currentIdentityName(record)})
		}
	}
	return e.asset, nil
}

func (e *entityEmitter) enabled(record *recordLowering) bool {
	return record != nil && record.plan.Entity != nil && !record.plan.Auxiliary
}
func (e *entityEmitter) stateName(record *recordLowering) string {
	return fmt.Sprintf("%sOriginal%d", e.prefix, record.order)
}
func (e *entityEmitter) recordsName(record *recordLowering) string {
	return fmt.Sprintf("records%d", record.order)
}
func (e *entityEmitter) childrenName(record *recordLowering) string {
	return fmt.Sprintf("children%d", record.order)
}
func (e *entityEmitter) captureName(record *recordLowering) string {
	return fmt.Sprintf("capture%d", record.order)
}
func (e *entityEmitter) statePointer(record *recordLowering) ast.Expr {
	return &ast.StarExpr{X: ast.NewIdent(e.stateName(record))}
}
func (e *entityEmitter) stateSlice(record *recordLowering) ast.Expr {
	return &ast.ArrayType{Elt: e.statePointer(record)}
}
func (e *entityEmitter) recordsType(record *recordLowering) ast.Expr {
	return &ast.MapType{Key: record.value.pointerExpr(), Value: e.statePointer(record)}
}
func (e *entityEmitter) keys(record *recordLowering) []plan.KeyPart {
	return record.plan.IdentityKeys()
}

func (e *entityEmitter) snapshotDeclaration() (ast.Decl, error) {
	fields := []*ast.Field{namedField("Roots", e.stateSlice(e.l.recordByPlan[e.l.plan.Root]))}
	for _, record := range e.l.records {
		if e.enabled(record) {
			fields = append(fields, namedField(e.recordsName(record), e.recordsType(record)))
			fields = append(fields, namedField(e.statesName(record), e.stateSlice(record)))
			if e.identity != nil && record.plan.Current != nil {
				fields = append(fields, namedField(e.identity.previousName(record), e.identity.previousType(e, record)))
			}
		}
	}
	accessFields, err := e.accessPlanFields()
	if err != nil {
		return nil, err
	}
	fields = append(fields, accessFields...)
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.snapshot), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}, nil
}

func (e *entityEmitter) originalDeclaration(record *recordLowering) (ast.Decl, error) {
	var markerFields []*ast.Field
	for _, field := range record.plan.Entity.Fields {
		markerFields = append(markerFields, namedField(field.Name, ast.NewIdent("bool")))
	}
	fields := []*ast.Field{namedField("marker", &ast.StructType{Fields: &ast.FieldList{List: markerFields}}), namedField("available", ast.NewIdent("bool")), namedField("original", record.value.pointerExpr()), namedField("source", record.value.pointerExpr()), namedField("owner", &ast.StarExpr{X: ast.NewIdent(e.snapshot)}), namedField("attached", ast.NewIdent("bool")), namedField("valueScopeSize", ast.NewIdent("int"))}
	if e.identity != nil {
		fields = append(fields, e.identity.captureFields(e, record)...)
	}
	for _, key := range e.keys(record) {
		typ, err := e.l.keyType(key.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, namedField("key"+key.Field, parseExpr(typ)), namedField("key"+key.Field+"Valid", ast.NewIdent("bool")))
	}
	relations, err := e.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for _, relation := range relations {
		fields = append(fields, namedField(relation.storage, e.stateSlice(relation.child)))
	}
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.stateName(record)), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}, nil
}

func (e *entityEmitter) availableDeclaration(record *recordLowering) ast.Decl {
	return &ast.FuncDecl{Name: ast.NewIdent("Available"), Recv: &ast.FieldList{List: []*ast.Field{namedField("s", e.statePointer(record))}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.BinaryExpr{X: &ast.BinaryExpr{X: ast.NewIdent("s"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Op: token.LAND, Y: selectExpr(ast.NewIdent("s"), "available")})}}}
}

func (e *entityEmitter) hasDeclaration(record *recordLowering) ast.Decl {
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("s"), "Available"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"))}}}}
	cases := []ast.Stmt{}
	for _, field := range record.plan.Entity.Fields {
		cases = append(cases, &ast.CaseClause{List: []ast.Expr{stringExpr(field.Name)}, Body: []ast.Stmt{returnStmt(selectExpr(selectExpr(ast.NewIdent("s"), "marker"), field.Name))}})
	}
	body = append(body, &ast.SwitchStmt{Tag: ast.NewIdent("field"), Body: &ast.BlockStmt{List: cases}}, returnStmt(ast.NewIdent("false")))
	return &ast.FuncDecl{Name: ast.NewIdent("Has"), Recv: &ast.FieldList{List: []*ast.Field{namedField("s", e.statePointer(record))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("field", ast.NewIdent("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) presenceAssertion(record *recordLowering) ast.Decl {
	return &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("_")}, Type: selectExpr(ast.NewIdent(e.l.handlerAlias), "OriginalPresence"), Values: []ast.Expr{callExpr(e.statePointer(record), ast.NewIdent("nil"))}}}}
}

func (e *entityEmitter) importDeclaration(file *ast.File) ast.Decl {
	imports := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		if selected, ok := node.(*ast.SelectorExpr); ok {
			if qualifier, ok := selected.X.(*ast.Ident); ok {
				if qualifier.Name == e.l.contextAlias {
					imports[qualifier.Name] = "context"
				}
				if qualifier.Name == e.l.handlerAlias {
					imports[qualifier.Name] = handlerPackage
				}
				if location := e.l.pathsByAlias[qualifier.Name]; location != "" && location != "#generated" {
					imports[qualifier.Name] = location
				}
			}
		}
		return true
	})
	aliases := make([]string, 0, len(imports))
	for alias := range imports {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	result := &ast.GenDecl{Tok: token.IMPORT, Lparen: 1}
	for _, alias := range aliases {
		result.Specs = append(result.Specs, &ast.ImportSpec{Name: ast.NewIdent(alias), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(imports[alias])}})
	}
	return result
}
