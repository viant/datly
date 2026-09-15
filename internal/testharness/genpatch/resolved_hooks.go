package genpatch

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ObserveResolvedHooks authors application behavior in the existing create-once
// lifecycle file. Regeneration must preserve this file byte for byte.
func ObserveResolvedHooks(t testing.TB, directory string) string {
	t.Helper()
	path := filepath.Join(directory, "lifecycle.go")
	content := ObserveHooks(t, directory)
	file, err := parser.ParseFile(token.NewFileSet(), path, content, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var root string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "Finalize" {
			root = fn.Recv.List[0].Type.(*ast.StarExpr).X.(*ast.Ident).Name
		}
	}
	body := func(text string) *ast.BlockStmt {
		parsed, err := parser.ParseFile(token.NewFileSet(), "body.go", "package orders;func f(){"+text+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		return parsed.Decls[0].(*ast.FuncDecl).Body
	}
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.TYPE {
			for _, spec := range gen.Specs {
				typ := spec.(*ast.TypeSpec)
				if typ.Name.Name == root {
					typ.Type.(*ast.StructType).Fields.List = append(typ.Type.(*ast.StructType).Fields.List, &ast.Field{Names: []*ast.Ident{ast.NewIdent("Input")}, Type: &ast.StarExpr{X: ast.NewIdent("OrdersInput")}, Tag: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(`bind:"kind=input"`)}})
				}
			}
		}
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		receiver := fn.Recv.List[0].Type.(*ast.StarExpr).X.(*ast.Ident).Name
		if receiver == root && fn.Name.Name == "Validate" {
			fn.Body = body(`indexes,err:=hooks.Input.ReadIndexes(ctx);if err!=nil{return err};groups:=indexes.CurrentKinds.GroupByName();if len(indexes.CurrentKinds)>0&&!groups.Has("standard"){return fmt.Errorf("grouped auxiliary lookup missing in Validate")};groupedValidated++;return nil`)
		}
		if strings.Contains(receiver, "Item") && fn.Name.Name == "Init" {
			fn.Body = body(`if entity.Name!=nil&&!state.Original.Has("Id") {
    switch *entity.Name {
    case "updated":if state.Previous==nil||state.Previous.Id==nil||*state.Previous.Id!=10||state.Previous.Name==nil||*state.Previous.Name!="old"{return fmt.Errorf("resolved update lost Previous or used public helper authority")};resolvedChildSeen=true
    case "preallocated":if state.Previous!=nil{return fmt.Errorf("preallocated ID became update")};preallocatedChildSeen=true
    }
   };return nil`)
		}
	}
	imports := &ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{&ast.ImportSpec{Path: &ast.BasicLit{Kind: token.STRING, Value: `"fmt"`}}}}
	file.Decls = append([]ast.Decl{imports}, file.Decls...)
	var result bytes.Buffer
	if err = format.Node(&result, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	edited := result.String() + "\nvar resolvedChildSeen, preallocatedChildSeen bool\nvar groupedValidated int\n"
	if err = os.WriteFile(path, []byte(edited), 0644); err != nil {
		t.Fatal(err)
	}
	return edited
}
