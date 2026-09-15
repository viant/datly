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
	Types                           xshape.Lookup
	Package, PackagePath, InputType string
	Owned                           bool
}

// ReadIndexAsset is application lookup syntax, separate from Previous authority.
type ReadIndexAsset struct {
	File                    *ast.File
	TypeName, BuildFunction string
	PackagePath             string
	CacheField              string
}

type readIndexEmitter struct{ previousEmitter }

func (l *lowerer) readIndexes() (*ReadIndexAsset, error) {
	if len(l.plan.ReadCollections) == 0 {
		return nil, nil
	}
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
	e := &readIndexEmitter{previousEmitter: previousEmitter{l: l}}
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	e.reflect = l.availableAlias("reflect")
	l.pathsByAlias[e.reflect] = "reflect"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	name := strings.TrimPrefix(l.factory, "New") + "ReadIndexes"
	asset := &ReadIndexAsset{TypeName: name, BuildFunction: "Build" + name, PackagePath: l.config.PackagePath}
	if target := l.config.ReadIndexes; target != nil && target.Owned {
		asset.CacheField = "_" + lowerInitial(name)
	}
	file := &ast.File{Name: ast.NewIdent(l.config.Package)}
	var fields []*ast.Field
	var build []ast.Stmt
	id := ast.NewIdent
	nilExpr := id("nil")
	build = append(build, &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read indexes require input"))}}}, &ast.AssignStmt{Lhs: []ast.Expr{id("metadata"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id(l.handlerAlias), "ReadMetadataFromContext"), id("ctx"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: id("ok")}, Op: token.LOR, Y: e.isNil(id("metadata"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read indexes require bound read metadata"))}}}, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: id(name)}}))
	for _, read := range l.plan.ReadCollections {
		if !token.IsIdentifier(read.Name) || !token.IsExported(read.Name) || len(read.InputPath) < 2 || read.InputPath[0] != "Input" {
			return nil, fmt.Errorf("invalid canonical application read %q", read.Name)
		}
		prefix := strings.TrimPrefix(l.factory, "New") + read.Name
		rowExpr, err := l.typeReference(read.Type)
		if err != nil {
			return nil, err
		}
		row, err := renderExpr(rowExpr)
		if err != nil {
			return nil, err
		}
		slice := prefix + "Slice"
		if err = l.reserveDeclaration(slice); err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(slice), Type: &ast.ArrayType{Elt: &ast.StarExpr{X: parseExpr(row)}}}}})
		fields = append(fields, namedField(read.Name, id(slice)))
		var initialize []ast.Stmt
		for _, field := range read.Fields {
			// Only declared comparable SQL scalar projections acquire map helpers.
			fieldExpr, err := l.typeReference(field.Type)
			if err != nil {
				return nil, err
			}
			rendered, err := renderExpr(fieldExpr)
			if err != nil {
				return nil, err
			}
			field.Type = spec.TypeRef{Name: rendered}
			typ, err := l.keyType(field.Type)
			if err != nil {
				continue
			}
			comparable, err := e.comparable(typ)
			if err != nil {
				return nil, fmt.Errorf("read %s index field %s: %w", read.Name, field.Field, err)
			}
			if !comparable {
				continue
			}

			part := plan.KeyPart{Field: field.Field, Source: field.Source, Type: field.Type}
			decls, err := e.indexMethods(slice, prefix, recordShape{base: row, pointer: true, many: true}, []plan.KeyPart{part}, field.Field, typ)
			if err != nil {
				return nil, err
			}
			file.Decls = append(file.Decls, decls...)

			if len(read.Keys) == 1 && read.Keys[0].Field == field.Field {
				keyField := read.Name + "By" + field.Field
				fields = append(fields, namedField(keyField, id(prefix+"IndexedBy"+field.Field)))
				initialize = append(initialize, &ast.AssignStmt{Lhs: []ast.Expr{selectExpr(id("result"), keyField), id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(id("result"), read.Name), "IndexBy"+field.Field))}}, e.guardError(nilExpr))
			}

		}
		read.Keys = append([]plan.KeyPart(nil), read.Keys...)
		for i := range read.Keys {
			expression, err := l.typeReference(read.Keys[i].Type)
			if err != nil {
				return nil, err
			}
			rendered, err := renderExpr(expression)
			if err != nil {
				return nil, err
			}
			read.Keys[i].Type = spec.TypeRef{Name: rendered}
			typ, err := l.keyType(read.Keys[i].Type)
			if err != nil {
				return nil, err
			}
			comparable, err := e.comparable(typ)
			if err != nil {
				return nil, err
			}
			if !comparable {
				return nil, fmt.Errorf("read %s primary key %s is not comparable", read.Name, read.Keys[i].Field)
			}

		}
		if len(read.Keys) > 1 {
			key, err := l.compileKey(read.Keys, prefix+"Key")
			if err != nil {
				return nil, err
			}
			file.Decls = append(file.Decls, key.declaration())
			decls, err := e.indexMethods(slice, prefix, recordShape{base: row, pointer: true, many: true}, read.Keys, "Key", key.name)
			if err != nil {
				return nil, err
			}
			file.Decls = append(file.Decls, decls...)
			fields = append(fields, namedField(read.Name+"ByKey", id(prefix+"IndexedByKey")))
			initialize = append(initialize, &ast.AssignStmt{Lhs: []ast.Expr{selectExpr(id("result"), read.Name+"ByKey"), id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(id("result"), read.Name), "IndexByKey"))}}, e.guardError(nilExpr))

		}
		groupDecls, groupFields, groupInit, err := e.requiredGroups(read, prefix, slice, row)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, groupDecls...)
		fields = append(fields, groupFields...)
		initialize = append(initialize, groupInit...)
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
