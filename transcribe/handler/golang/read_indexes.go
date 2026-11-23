package golang

import (
	"fmt"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// ReadIndexConfig selects the input's package authority for application helpers.
type ReadIndexConfig struct {
	Types                            xshape.Lookup
	Package, PackagePath, InputType  string
	Owned                            bool
	HelperPackage, HelperPackagePath string
	ProjectRoot                      string
}

// ReadIndexAsset is application lookup syntax, separate from Previous authority.
type ReadIndexAsset struct {
	File                    *ast.File
	Helpers                 *ast.File
	TypeName, BuildFunction string
	PackagePath             string
	CacheField              string
}

type readIndexEmitter struct {
	previousEmitter
	loops *ownerIndexLoops
}

// ReadIndexes emits only the public typed read-index support used by input and
// entity hooks. It does not emit a component-specific mutation program.
func ReadIndexes(value *plan.Plan, config Config) (*ReadIndexAsset, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	return l.readIndexes()
}

func (l *lowerer) readIndexes() (*ReadIndexAsset, error) {
	if len(l.plan.ReadCollections) == 0 {
		return nil, nil
	}
	originalConfig := l.config
	if l.config.ReadIndexes != nil {
		config := l.config
		target := config.ReadIndexes
		isolated := &lowerer{plan: l.plan, config: config}
		if err := isolated.prepare(); err != nil {
			return nil, err
		}
		isolated.config.Package = target.Package
		isolated.config.PackagePath = target.PackagePath
		input, err := isolated.typeReference(spec.TypeRef{Name: target.InputType})
		if err != nil {
			return nil, err
		}
		isolated.config.InputType, err = renderExpr(input)
		if err != nil {
			return nil, err
		}
		l = isolated
	}
	e := newReadIndexEmitter(l)
	helpers := e
	if target := l.config.ReadIndexes; target != nil && target.HelperPackagePath != "" && target.HelperPackagePath != l.config.PackagePath {
		other := &lowerer{plan: l.plan, config: originalConfig}
		// Prepare under the component's original import authority, then render
		// row and key types under the independent helper package's authority.
		if err := other.prepare(); err != nil {
			return nil, err
		}
		other.config.Package, other.config.PackagePath = target.HelperPackage, target.HelperPackagePath
		helpers = newReadIndexEmitter(other)
	}
	name := strings.TrimPrefix(l.factory, "New") + "ReadIndexes"
	asset := &ReadIndexAsset{TypeName: name, BuildFunction: "Build" + name, PackagePath: l.config.PackagePath}
	if target := l.config.ReadIndexes; target != nil && target.Owned {
		asset.CacheField = "_" + lowerInitial(name)
	}
	file := &ast.File{Name: ast.NewIdent(l.config.Package)}
	helperFile := file
	if helpers != e {
		helperFile = &ast.File{Name: ast.NewIdent(helpers.l.config.Package)}
	}
	var fields []*ast.Field
	var build []ast.Stmt
	id := ast.NewIdent
	nilExpr := id("nil")
	build = append(build, &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read indexes require input"))}}}, &ast.AssignStmt{Lhs: []ast.Expr{id("metadata"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id(l.handlerAlias), "ReadMetadataFromContext"), id("ctx"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: id("ok")}, Op: token.LOR, Y: e.isNil(id("metadata"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read indexes require bound read metadata"))}}}, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: id(name)}}))
	var loops *ownerIndexLoops
	var graph *indexImportGraph
	if helpers != e {
		var err error
		graph, err = newIndexImportGraph(l.config.ReadIndexes.ProjectRoot, l.config.PackagePath)
		if err != nil {
			return nil, err
		}
	}
	for _, read := range l.plan.ReadCollections {
		helpers, helperFile := helpers, helperFile
		if helpers != e {
			local, err := graph.ownerCollection(helpers.l, read)
			if err != nil {
				return nil, err
			}
			if local {
				if loops == nil {
					loops, err = newOwnerIndexLoops(e.l, helpers.l)
					if err != nil {
						return nil, err
					}
				}
				localEmitter := *e
				localEmitter.loops = loops
				helpers, helperFile = &localEmitter, file
			}
		}
		if !token.IsIdentifier(read.Name) || !token.IsExported(read.Name) || len(read.InputPath) < 2 || read.InputPath[0] != "Input" {
			return nil, fmt.Errorf("invalid canonical application read %q", read.Name)
		}
		prefix := strings.TrimPrefix(l.factory, "New") + read.Name
		rowExpr, err := helpers.l.typeReference(read.Type)
		if err != nil {
			return nil, err
		}
		row, err := renderExpr(rowExpr)
		if err != nil {
			return nil, err
		}
		slice := prefix + "Slice"
		if err = helpers.l.reserveDeclaration(slice); err != nil {
			return nil, err
		}
		helperFile.Decls = append(helperFile.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(slice), Type: &ast.ArrayType{Elt: &ast.StarExpr{X: parseExpr(row)}}}}})
		fields = append(fields, namedField(read.Name, id(slice)))
		var initialize []ast.Stmt
		for _, field := range read.Fields {
			// Only declared comparable SQL scalar projections acquire map helpers.
			fieldExpr, err := helpers.l.typeReference(field.Type)
			if err != nil {
				return nil, err
			}
			rendered, err := renderExpr(fieldExpr)
			if err != nil {
				return nil, err
			}
			field.Type = spec.TypeRef{Name: rendered}
			typ, err := helpers.l.keyType(field.Type)
			if err != nil {
				continue
			}
			comparable, err := helpers.comparable(typ)
			if err != nil {
				return nil, fmt.Errorf("read %s index field %s: %w", read.Name, field.Field, err)
			}
			if !comparable {
				continue
			}

			part := plan.KeyPart{Field: field.Field, Source: field.Source, Type: field.Type}
			decls, err := helpers.indexMethods(slice, prefix, recordShape{base: row, pointer: true, many: true}, []plan.KeyPart{part}, field.Field, typ)
			if err != nil {
				return nil, err
			}
			helperFile.Decls = append(helperFile.Decls, decls...)

			if len(read.Keys) == 1 && read.Keys[0].Field == field.Field {
				keyField := read.Name + "By" + field.Field
				fields = append(fields, namedField(keyField, id(prefix+"IndexedBy"+field.Field)))
				initialize = append(initialize, &ast.AssignStmt{Lhs: []ast.Expr{selectExpr(id("result"), keyField), id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(id("result"), read.Name), "IndexBy"+field.Field))}}, e.guardError(nilExpr))
			}

		}
		read.Keys = append([]plan.KeyPart(nil), read.Keys...)
		for i := range read.Keys {
			expression, err := helpers.l.typeReference(read.Keys[i].Type)
			if err != nil {
				return nil, err
			}
			rendered, err := renderExpr(expression)
			if err != nil {
				return nil, err
			}
			read.Keys[i].Type = spec.TypeRef{Name: rendered}
			typ, err := helpers.l.keyType(read.Keys[i].Type)
			if err != nil {
				return nil, err
			}
			comparable, err := helpers.comparable(typ)
			if err != nil {
				return nil, err
			}
			if !comparable {
				return nil, fmt.Errorf("read %s primary key %s is not comparable", read.Name, read.Keys[i].Field)
			}

		}
		if len(read.Keys) > 1 {
			key, err := helpers.l.compileKey(read.Keys, prefix+"Key")
			if err != nil {
				return nil, err
			}
			helperFile.Decls = append(helperFile.Decls, key.declaration())
			decls, err := helpers.indexMethods(slice, prefix, recordShape{base: row, pointer: true, many: true}, read.Keys, "Key", key.name)
			if err != nil {
				return nil, err
			}
			helperFile.Decls = append(helperFile.Decls, decls...)
			fields = append(fields, namedField(read.Name+"ByKey", id(prefix+"IndexedByKey")))
			initialize = append(initialize, &ast.AssignStmt{Lhs: []ast.Expr{selectExpr(id("result"), read.Name+"ByKey"), id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(id("result"), read.Name), "IndexByKey"))}}, e.guardError(nilExpr))

		}
		groupDecls, groupFields, groupInit, err := helpers.requiredGroups(read, prefix, slice, row)
		if err != nil {
			return nil, err
		}
		helperFile.Decls = append(helperFile.Decls, groupDecls...)
		fields = append(fields, groupFields...)
		initialize = append(initialize, groupInit...)
		if helpers.l != e.l {
			inputRow, err := l.typeReference(read.Type)
			if err != nil {
				return nil, err
			}
			row, err = renderExpr(inputRow)
			if err != nil {
				return nil, err
			}
		}
		block, err := e.captureRead(read, row, slice)
		if err != nil {
			return nil, err
		}
		build = append(build, &ast.BlockStmt{List: append(block, initialize...)})
	}
	names := map[string]bool{}
	for _, field := range fields {
		name := field.Names[0].Name
		if names[name] {
			return nil, fmt.Errorf("application read collection field %s collides", name)
		}
		names[name] = true
	}
	if helpers != e {
		if err := e.retainIndexAPI(file, helperFile, helpers.l); err != nil {
			return nil, err
		}
		if loops != nil {
			helperFile.Decls = append(helperFile.Decls, loops.declarations...)
		}
		helperFile.Decls = append([]ast.Decl{(&entityEmitter{l: helpers.l}).importDeclaration(helperFile)}, helperFile.Decls...)
		asset.Helpers = helperFile
	}
	file.Decls = append(file.Decls, e.structure(name, fields...))
	build = append(build, returnStmt(id("result"), nilExpr))
	file.Decls = append(file.Decls, e.function(asset.BuildFunction, []*ast.Field{namedField("ctx", selectExpr(id(l.contextAlias), "Context")), namedField("input", &ast.StarExpr{X: parseExpr(l.config.InputType)})}, []*ast.Field{{Type: &ast.StarExpr{X: id(name)}}, {Type: id("error")}}, build))
	if asset.CacheField != "" {
		receiver := &ast.FieldList{List: []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(l.config.InputType)})}}
		cache := selectExpr(id("input"), asset.CacheField)
		missing := &ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: nilExpr}
		prepare := e.function("PrepareReadIndexes", []*ast.Field{namedField("ctx", selectExpr(id(l.contextAlias), "Context"))}, []*ast.Field{{Type: id("error")}}, []ast.Stmt{&ast.IfStmt{Cond: missing, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("read indexes require input"))}}}, assignStmt(cache, nilExpr), &ast.AssignStmt{Lhs: []ast.Expr{id("indexes"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(asset.BuildFunction), id("ctx"), id("input"))}}, errorGuard(id("err")), assignStmt(cache, id("indexes")), returnStmt(nilExpr)})
		prepare.Recv = receiver
		method := e.function("ReadIndexes", []*ast.Field{namedField("ctx", selectExpr(id(l.contextAlias), "Context"))}, []*ast.Field{{Type: &ast.StarExpr{X: id(name)}}, {Type: id("error")}}, []ast.Stmt{&ast.IfStmt{Cond: missing, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read indexes require input"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: cache, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(id("input"), "PrepareReadIndexes"), id("ctx"))), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, id("err"))}}}}}}, returnStmt(cache, nilExpr)})
		method.Recv = receiver
		file.Decls = append(file.Decls, prepare, method)
	}

	file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
	asset.File = file
	return asset, nil
}

func (e *readIndexEmitter) retainIndexAPI(file, helpers *ast.File, helper *lowerer) error {
	owner := e.l
	helperPath := helper.config.PackagePath
	for _, decl := range helpers.Decls {
		// Methods remain available through the type aliases. Package-level
		// key functions need a forwarding declaration of their own.
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.IsExported() {
			target, err := owner.typeReference(spec.TypeRef{Package: helperPath, Name: fn.Name.Name})
			if err != nil {
				return err
			}
			arg, err := e.indexSignatureType(helper, fn.Type.Params.List[0].Type)
			if err != nil {
				return err
			}
			key, err := e.indexSignatureType(helper, fn.Type.Results.List[0].Type)
			if err != nil {
				return err
			}
			file.Decls = append(file.Decls, e.function(fn.Name.Name, []*ast.Field{namedField("value", arg)},
				[]*ast.Field{{Type: key}, {Type: ast.NewIdent("bool")}},
				[]ast.Stmt{returnStmt(callExpr(target, ast.NewIdent("value")))}))
			continue
		}
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.TYPE {
			continue
		}
		for _, item := range group.Specs {
			name := item.(*ast.TypeSpec).Name.Name
			ref, err := owner.typeReference(spec.TypeRef{Package: helperPath, Name: name})
			if err != nil {
				return err
			}
			file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(name), Assign: token.Pos(1), Type: ref}}})
		}
	}
	return nil
}

// Render the helper's signature in the input owner's import namespace.
func (e *readIndexEmitter) indexSignatureType(helper *lowerer, expr ast.Expr) (ast.Expr, error) {
	source, err := renderExpr(expr)
	if err != nil {
		return nil, err
	}
	canonical, err := (xshape.Resolver{Package: helper.config.PackagePath, Imports: helper.pathsByAlias}).Canonical(source)
	if err != nil {
		return nil, err
	}
	return e.l.typeReference(spec.TypeRef{Name: canonical})
}

func newReadIndexEmitter(l *lowerer) *readIndexEmitter {
	e := &readIndexEmitter{previousEmitter: previousEmitter{l: l}}
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	e.reflect = l.availableAlias("reflect")
	l.pathsByAlias[e.reflect] = "reflect"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	return e
}

func (e *readIndexEmitter) indexMethods(slice, prefix string, row recordShape, parts []plan.KeyPart, suffix, keyType string) ([]ast.Decl, error) {
	id := ast.NewIdent
	keyName := prefix + "IndexBy" + suffix + "Key"
	key := keyShape{parts: parts, types: []string{keyType}, name: keyType, compound: len(parts) > 1}
	declarations := []ast.Decl{e.l.keyFunction(keyName, row, key, parts)}
	for _, grouped := range []bool{false, true} {
		method, typ := "IndexBy"+suffix, prefix+"IndexedBy"+suffix
		value := row.pointerExpr()
		if grouped {
			method, typ = "GroupBy"+suffix, prefix+"GroupedBy"+suffix
			value = &ast.ArrayType{Elt: value}
		}
		if err := e.l.reserveDeclaration(typ); err != nil {
			return nil, err
		}
		declarations = append(declarations, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(typ), Type: &ast.MapType{Key: parseExpr(keyType), Value: value}}}})
		loop := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("key"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(keyName), id("row"))}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: id("ok")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}}}
		entry := &ast.IndexExpr{X: id("result"), Index: id("key")}
		if grouped {
			loop = append(loop, assignStmt(entry, callExpr(id("append"), entry, id("row"))))
		} else {
			loop = append(loop, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{id("_"), id("exists")}, Tok: token.DEFINE, Rhs: []ast.Expr{entry}}, Cond: id("exists"), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("nil"), e.errorExpr("ambiguous application index "+slice+"."+method))}}}, assignStmt(entry, id("row")))
		}
		results := []*ast.Field{{Type: id(typ)}}
		returns := []ast.Expr{id("result")}
		if !grouped {
			results = append(results, &ast.Field{Type: id("error")})
			returns = append(returns, id("nil"))
		}
		fn := e.function(method, nil, results, []ast.Stmt{defineStmt("result", callExpr(id("make"), id(typ))), &ast.RangeStmt{Key: id("_"), Value: id("row"), Tok: token.DEFINE, X: id("rows"), Body: &ast.BlockStmt{List: loop}}, returnStmt(returns...)})
		if e.loops != nil {
			fn.Body.List = []ast.Stmt{returnStmt(e.loops.call(grouped, typ, keyName, slice+"."+method))}
		}
		fn.Recv = &ast.FieldList{List: []*ast.Field{namedField("rows", id(slice))}}
		declarations = append(declarations, fn)
		has := e.function("Has", []*ast.Field{namedField("key", parseExpr(keyType))}, []*ast.Field{{Type: id("bool")}}, []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("_"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.IndexExpr{X: id("index"), Index: id("key")}}}, returnStmt(id("ok"))})
		has.Recv = &ast.FieldList{List: []*ast.Field{namedField("index", id(typ))}}
		declarations = append(declarations, has)
	}
	return declarations, nil
}

func (e *readIndexEmitter) comparable(expression string) (bool, error) {
	var lookup xshape.Lookup
	if e.l.config.ReadIndexes != nil {
		lookup = e.l.config.ReadIndexes.Types
	}
	return (xshape.Resolver{Package: e.l.config.PackagePath, Imports: e.l.pathsByAlias, Lookup: lookup}).IsComparable(expression)
}
