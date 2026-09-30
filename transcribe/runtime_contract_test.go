package transcribe

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/bootstrap"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
)

func TestCompilerRuntimeContractsMaterializeDynamicReaderTypes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/app\n\ngo 1.25.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	contract, err := NewCompiler().RuntimeContracts(context.Background(), root, &Source{
		Scope: "example.com/app/dynamic/vendors", Name: "Vendors", Text: `#package('example.com/app/dynamic/vendors')
#setting($_ = $connector('main'))
#setting($_ = $route('/vendors','GET'))
#define($_ = $Vendors<[]*Vendor>(output/view))
SELECT vendor.*, type(vendor, 'Vendor')
FROM (SELECT 1 AS ID, 'one' AS NAME) vendor`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if contract.Component == nil || contract.InputType == nil || contract.OutputType == nil || contract.Plan == nil {
		t.Fatalf("contract=%+v", contract)
	}
	field, ok := contract.OutputType.FieldByName("Vendors")
	if !ok || field.Type.Kind() != reflect.Slice || field.Type.Elem().Kind() != reflect.Ptr {
		t.Fatalf("Vendors output field=%+v", field)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources})
	if err != nil || artifact.ReaderCompilation() == nil {
		t.Fatalf("artifact=%+v err=%v", artifact, err)
	}
	db, err := sql.Open("sqlite3", "file:runtime-contract?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := execution.Read(context.Background(), reflect.New(contract.InputType).Interface(), nil, nil)
	if err != nil || actual == nil {
		t.Fatalf("actual=%+v err=%v", actual, err)
	}
}

func TestRuntimeContractsInModuleNeedsNoSourceCheckout(t *testing.T) {
	root := t.TempDir() // deliberately no go.mod
	source := &Source{Scope: "example.com/app/dynamic/sqltest", Name: "preview", Text: `#package('example.com/app/dynamic/sqltest')
#setting($_ = $route('/preview','GET'))
#define($_ = $Rows<[]*Row>(output/view))
SELECT result.*, type(result, 'Row') FROM (SELECT 1 AS ID) result`}
	contract, err := NewCompiler().RuntimeContractsInModule(context.Background(), "example.com/app", source)
	if err != nil || contract == nil || contract.InputType == nil || contract.OutputType == nil {
		t.Fatalf("source-free runtime contract=%+v err=%v", contract, err)
	}
	if _, err = NewCompiler().RuntimeContracts(context.Background(), root, source); err == nil {
		t.Fatal("source-checkout contract unexpectedly accepted a directory without go.mod")
	}
	outside := *source
	outside.Text = strings.ReplaceAll(source.Text, "example.com/app/dynamic/sqltest", "other.example/app/dynamic/sqltest")
	if _, err = NewCompiler().RuntimeContractsInModule(context.Background(), "example.com/app", &outside); err == nil {
		t.Fatal("source-free runtime contract accepted an outside-module package")
	}
}

func TestRuntimeContractsRefineDynamicWherePredicate(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/app\n\ngo 1.25.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "vendors.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE VENDOR (ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	contract, err := NewCompiler().RuntimeContracts(context.Background(), root, &Source{
		Scope: "example.com/app/dynamic/vendors", Name: "Vendors", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db}), Text: `#package('example.com/app/dynamic/vendors')
#setting($_ = $connector('main'))
#setting($_ = $route('/vendors','GET'))
#define($_ = $Limit<int>(query/limit).Optional().WithPredicate(1, 'less_or_equal', 't', 'ID'))
#define($_ = $Vendors<[]*Vendor>(output/view))
SELECT vendor.*, type(vendor, 'Vendor')
FROM (SELECT * FROM VENDOR t
${predicate.Builder().CombineAnd($predicate.FilterGroup(1, "AND")).Build("WHERE")}
) vendor`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if field, ok := contract.OutputType.FieldByName("Vendors"); !ok || field.Type.Elem().Elem().NumField() == 0 {
		t.Fatalf("output type=%v field=%+v", contract.OutputType, field)
	}
}
