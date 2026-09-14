package compiler

import (
	"reflect"
	"strings"
	"testing"

	dtag "github.com/viant/datly/tag"
)

func TestParseLinkOnUsesCanonicalTypeFieldResolution(t *testing.T) {
	type parent struct {
		AccountKey int
	}
	type child struct {
		AccountKey int
	}

	metadata, err := dtag.ParseRelation("p.account_key=c.account_key")
	if err != nil {
		t.Fatal(err)
	}
	links, err := parseLinkOn(metadata, reflect.TypeOf(parent{}), reflect.TypeOf(child{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].parentField != "AccountKey" || links[0].parentNamespace != "p" ||
		links[0].childField != "AccountKey" || links[0].childNamespace != "c" {
		t.Fatalf("links = %+v", links)
	}
}

func TestParseLinkOnRejectsUnsupportedIncludeFlag(t *testing.T) {
	type row struct {
		AccountID int
	}

	metadata, err := dtag.ParseRelation("AccountID:account_id(true)=AccountID:account_id")
	if err != nil {
		t.Fatal(err)
	}
	_, err = parseLinkOn(metadata, reflect.TypeOf(row{}), reflect.TypeOf(row{}))
	if err == nil || !strings.Contains(err.Error(), "include flag is not supported") {
		t.Fatalf("parseLinkOn() error = %v", err)
	}
}

func TestParseLinkOnPreservesExplicitHiddenField(t *testing.T) {
	type row struct {
		AccountID int
	}

	metadata, err := dtag.ParseRelation("HiddenAccountID:account_id=AccountID:account_id")
	if err != nil {
		t.Fatal(err)
	}
	links, err := parseLinkOn(metadata, reflect.TypeOf(row{}), reflect.TypeOf(row{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].parentField != "HiddenAccountID" || links[0].childField != "AccountID" {
		t.Fatalf("links = %+v", links)
	}
}

func TestParseLinkOnSQLColumnAliases(t *testing.T) {
	type row struct {
		ID int `sqlx:"record_key"`
	}
	type ambiguous struct {
		First  int `sqlx:"record_key"`
		Second int `sqlx:"record_key"`
	}
	for _, test := range []struct {
		name    string
		typ     reflect.Type
		failure bool
	}{{"sqlx_alias", reflect.TypeOf(row{}), false}, {"ambiguous_alias", reflect.TypeOf(ambiguous{}), true}} {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := dtag.ParseRelation("p.record_key=c.record_key")
			if err != nil {
				t.Fatal(err)
			}
			links, err := parseLinkOn(metadata, test.typ, test.typ)
			if test.failure {
				if err == nil || !strings.Contains(err.Error(), "ambiguous") {
					t.Fatalf("alias error %v", err)
				}
				return
			}
			if err != nil || len(links) != 1 || links[0].parentField != "ID" || links[0].childField != "ID" || links[0].parentColumn != "record_key" {
				t.Fatalf("links %+v error %v", links, err)
			}
		})
	}
}
