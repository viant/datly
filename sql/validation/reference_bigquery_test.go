package validation_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/validation"
	sqlvalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
)

func TestMatchReferenceBigQueryIdentifierAuthority(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// SQLite supplies only a transaction handle for this no-query identity test.
	// This is not BigQuery transaction or driver execution evidence.
	for _, product := range []string{"BigQuery", "SQLServer"} {
		service := validation.New(referenceConnection{validation.Connection{DB: h.DB, Tx: tx, Dialect: &info.Dialect{Product: database.Product{Name: product}}}})
		for _, table := range []string{"`project.dataset.parents`", "[project.dataset.parents]", "[project:dataset.parents]"} {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Parent", Type: reflect.TypeOf(0), Tag: reflect.StructTag("sqlx:" + strconv.Quote("parent_id,refTable="+table+",refColumn=id"))}})
			child := reflect.New(typ)
			child.Elem().Field(0).SetInt(7)
			parent := &struct {
				ID int `sqlx:"id"`
			}{7}
			for _, tc := range []struct {
				schema, table string
				match         bool
			}{
				{"", "project.dataset.parents", product == "BigQuery"}, {"project.dataset", "parents", product == "BigQuery"},
				{"", "other.dataset.parents", false}, {"", "project.other.parents", false}, {"", "parents", false},
			} {
				actual, err := service.MatchReference(ctx, child.Interface(), parent, xhandler.ValidationReference{Field: "Parent", Schema: tc.schema, Table: tc.table, Column: "id"})
				if err != nil || (actual != nil) != tc.match {
					t.Fatalf("%s %s -> %s/%s: %+v %v", product, table, tc.schema, tc.table, actual, err)
				}
				if actual != nil && actual.Table != table {
					t.Fatalf("native SQL descriptor changed: %+v", actual)
				}
			}
		}
	}
}

func TestMatchReferenceQuotedSchemaKeepsNativeReceipt(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	service := validation.New(referenceConnection{validation.Connection{DB: h.DB, Tx: tx}})
	child := &struct {
		Parent int `sqlx:"parent_id,refDb=\"main\",refTable=parents,refColumn=id"`
	}{7}
	parent := &struct {
		ID int `sqlx:"id"`
	}{7}
	actual, err := service.MatchReference(ctx, child, parent, xhandler.ValidationReference{Field: "Parent", Schema: "main", Table: "parents", Column: "id"})
	if err != nil || actual == nil || actual.Schema != `"main"` {
		t.Fatalf("native receipt=%+v error=%v", actual, err)
	}
	checks, err := sqlvalidator.NewChecks(reflect.TypeOf(child), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checks.ValidateReferences([]sqlvalidator.Reference{{Field: actual.Field, Schema: actual.Schema, Table: actual.Table, Column: actual.Column}}); err != nil {
		t.Fatal(err)
	}
}
