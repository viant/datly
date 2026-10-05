package compile

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestWildcardRenameMetadataStatusOwnership(t *testing.T) {
	for _, projection := range []string{
		`r.*,r.STATUS AS PersistedStatus,r.LOGICAL_STATUS AS Status`,
		`r.LOGICAL_STATUS AS Status,r.STATUS AS PersistedStatus,r.*`,
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse_metadata=%v", projection, reverse), func(t *testing.T) {
				groupable, defaultValue := false, "1"
				physical := &spec.Column{
					Name: "STATUS", Source: "STATUS", Type: spec.TypeRef{Package: "example.com/model", Name: "State", Pointer: true},
					ExplicitType: true, NameInferred: true, DatabaseType: "INTEGER", Expression: "original",
					Nullable: true, Optional: true, NotNull: true, Groupable: &groupable, PrimaryKey: true,
					AutoIncrement: true, Unique: true, Default: &defaultValue, DeleteMarker: true, ConcurrencyToken: true,
					Tag:   `sqlx:"STATUS" json:"-" invariant:"State" internal:"true"`,
					Codec: &spec.Codec{Body: "codec", OutputType: "string", Args: []string{"arg"}},
				}
				logical := &spec.Column{Name: "LOGICAL_STATUS", Source: "LOGICAL_STATUS", ExplicitType: true, Required: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"LOGICAL_STATUS" json:"status"`}
				wantPhysical, wantLogical := physical.Clone(), logical.Clone()
				wantPhysical.Name, wantLogical.Name = "PersistedStatus", "Status"
				view := &spec.View{Name: "Records", Namespace: "r", Columns: []*spec.Column{physical, logical}}
				if reverse {
					view.Columns = []*spec.Column{logical, physical}
				}
				parsed, err := sqlparser.ParseQuery(`SELECT ` + projection + ` FROM (SELECT STATUS,LOGICAL_STATUS FROM RECORDS) r`)
				if err != nil {
					t.Fatal(err)
				}
				before := sqlparser.Stringify(parsed)
				result, err := readViewProjection(parsed, view, view)
				if err != nil {
					t.Fatal(err)
				}
				if len(result) != 0 || len(view.Columns) != 2 || !reflect.DeepEqual(physical, wantPhysical) || !reflect.DeepEqual(logical, wantLogical) {
					t.Fatalf("metadata ownership changed: physical=%+v logical=%+v count=%d projection=%v", physical, logical, len(view.Columns), result)
				}
				if sqlparser.Stringify(parsed) != before {
					t.Fatal("authored SQL AST mutated")
				}
			})
		}
	}
}

func TestWildcardRenameMetadataPhysicalDeclarations(t *testing.T) {
	for _, projection := range []string{
		`r.*,r.STATUS AS PersistedStatus,r.LOGICAL_STATUS AS Status`,
		`r.LOGICAL_STATUS AS Status,r.STATUS AS PersistedStatus,r.*`,
	} {
		for _, declarations := range []string{
			`CAST(r.STATUS AS int),tag(r.STATUS,'json:"-"'),invariant(r.STATUS,'State'),optional(r.STATUS),CAST(r.LOGICAL_STATUS AS string),tag(r.LOGICAL_STATUS,'json:"status"'),required(r.LOGICAL_STATUS)`,
			`required(r.LOGICAL_STATUS),tag(r.LOGICAL_STATUS,'json:"status"'),CAST(r.LOGICAL_STATUS AS string),optional(r.STATUS),invariant(r.STATUS,'State'),tag(r.STATUS,'json:"-"'),CAST(r.STATUS AS int)`,
		} {
			SQL := `SELECT ` + projection + `,` + declarations + ` FROM (SELECT STATUS,LOGICAL_STATUS FROM RECORDS) r`
			input := &spec.View{Name: "Records"}
			got, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Columns) != 2 || len(input.Columns) != 0 {
				t.Fatalf("duplicate metadata or authored mutation: %+v", got.Columns)
			}
			byName := map[string]*spec.Column{}
			for _, column := range got.Columns {
				byName[column.Name] = column
			}
			physical, logical := byName["PersistedStatus"], byName["Status"]
			if physical == nil || logical == nil || physical.Source != "STATUS" || logical.Source != "LOGICAL_STATUS" || !physical.ExplicitType || physical.Type.Name != "int" || !physical.Optional || !physical.Nullable || !logical.ExplicitType || logical.Type.Name != "string" || !logical.Required {
				t.Fatalf("physical/logical declarations: %+v", got.Columns)
			}
			physicalTags, logicalTags := reflect.StructTag(physical.Tag), reflect.StructTag(logical.Tag)
			if physicalTags.Get("json") != "-" || physicalTags.Get("invariant") != "State" || physicalTags.Get("sqlx") != "STATUS" || logicalTags.Get("json") != "status" || logicalTags.Get("sqlx") != "LOGICAL_STATUS" {
				t.Fatalf("annotation metadata lost: physical=%q logical=%q", physical.Tag, logical.Tag)
			}
			if got.Source.Table != "RECORDS" || !strings.Contains(got.Source.SQL, "SELECT STATUS,LOGICAL_STATUS FROM RECORDS") || strings.Contains(got.Source.SQL, "PersistedStatus") || strings.Contains(got.Source.SQL, "AS Status") {
				t.Fatalf("SQL identity changed: %+v", got.Source)
			}
		}
	}
}

func TestWildcardRenameMetadataSinglePhysicalRecord(t *testing.T) {
	SQL := `SELECT r.*,r.NAME AS DisplayName,CAST(r.NAME AS string),tag(r.NAME,'json:"display"'),invariant(r.NAME,'Label') FROM (SELECT NAME FROM RECORDS) r`
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 1 {
		t.Fatalf("physical annotation duplicated: %+v", got.Columns)
	}
	column := got.Columns[0]
	if column.Name != "DisplayName" || column.Source != "NAME" || column.Type.Name != "string" || !column.ExplicitType || reflect.StructTag(column.Tag).Get("json") != "display" || reflect.StructTag(column.Tag).Get("invariant") != "Label" || reflect.StructTag(column.Tag).Get("sqlx") != "NAME" {
		t.Fatalf("physical annotation not retained: %+v", column)
	}
}

func TestWildcardRenameMetadataExplicitAlias(t *testing.T) {
	for _, source := range []string{"", "DisplayName", "NAME"} {
		input := &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "DisplayName", Source: source, ExplicitType: true, Required: true, Type: spec.TypeRef{Name: "string"}, Tag: `json:"display"`, Codec: &spec.Codec{Body: "original"}}}}
		SQL := `SELECT r.*,r.NAME AS DisplayName FROM (SELECT * FROM RECORDS) r`
		got, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Columns) != 1 || got.Columns[0].Name != "DisplayName" || got.Columns[0].Source != "NAME" || !got.Columns[0].ExplicitType || !got.Columns[0].Required || got.Columns[0].Codec == nil || got.Columns[0].Codec.Body != "original" || reflect.StructTag(got.Columns[0].Tag).Get("json") != "display" || input.Columns[0].Source != source {
			t.Fatalf("legitimate alias metadata lost: %+v", got.Columns)
		}
	}
	SQL := `SELECT r.*,r.NAME AS DisplayName,CAST(r.DisplayName AS string),invariant(r.DisplayName,'Label') FROM (SELECT * FROM RECORDS) r`
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: SQL})
	if err != nil || len(got.Columns) != 1 || got.Columns[0].Name != "DisplayName" || got.Columns[0].Source != "NAME" || !got.Columns[0].ExplicitType || reflect.StructTag(got.Columns[0].Tag).Get("invariant") != "Label" {
		t.Fatalf("authored alias declaration failed: got=%+v err=%v", got, err)
	}
}

func TestWildcardRenameMetadataRejectsOwnershipConflicts(t *testing.T) {
	for _, tc := range []struct {
		name, projection, want string
		columns                []*spec.Column
	}{
		{"duplicate source", `r.*,r.NAME AS DisplayName`, "matches multiple canonical columns", []*spec.Column{{Name: "a", Source: "NAME"}, {Name: "b", Source: "name"}}},
		{"duplicate alias", `r.*,r.NAME AS DisplayName`, "alias DisplayName matches multiple canonical columns", []*spec.Column{{Name: "DisplayName"}, {Name: "displayname", Source: "DisplayName"}}},
		{"unrelated alias source", `r.*,r.NAME AS DisplayName`, "owned by unrelated source OTHER", []*spec.Column{{Name: "DisplayName", Source: "OTHER"}}},
		{"name cannot replace source", `r.*,r.NAME AS DisplayName`, "owned by unrelated source OTHER", []*spec.Column{{Name: "DisplayName", Source: "OTHER"}, {Name: "NAME", Source: "NAME"}}},
		{"competing physical and alias", `r.*,r.NAME AS DisplayName`, "competing canonical columns", []*spec.Column{{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "int"}}, {Name: "DisplayName", Source: "DisplayName", Type: spec.TypeRef{Name: "string"}}}},
		{"one source two aliases", `r.*,r.NAME AS First,r.NAME AS Second`, "ambiguous wildcard rename source", []*spec.Column{{Name: "NAME", Source: "NAME"}}},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", tc.name, reverse), func(t *testing.T) {
				columns := append([]*spec.Column(nil), tc.columns...)
				if reverse {
					for left, right := 0, len(columns)-1; left < right; left, right = left+1, right-1 {
						columns[left], columns[right] = columns[right], columns[left]
					}
				}
				view := &spec.View{Name: "Records", Namespace: "r", Columns: columns}
				before := view.Clone()
				parsed, err := sqlparser.ParseQuery(`SELECT ` + tc.projection + ` FROM (SELECT NAME,OTHER FROM RECORDS) r`)
				if err != nil {
					t.Fatal(err)
				}
				_, err = readViewProjection(parsed, view, view)
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("ownership accepted: %v", err)
				}
				if !reflect.DeepEqual(before, view) {
					t.Fatal("invalid ownership mutated metadata")
				}
			})
		}
	}
}

func TestWildcardRenameMetadataSourceAuthority(t *testing.T) {
	column := &spec.Column{Name: "NAME", Source: "OTHER", Type: spec.TypeRef{Name: "int"}, NameInferred: true}
	view := &spec.View{Name: "Records", Namespace: "r", Columns: []*spec.Column{column}}
	before := column.Clone()
	parsed, err := sqlparser.ParseQuery(`SELECT r.*,r.NAME AS DisplayName FROM (SELECT NAME,OTHER FROM RECORDS) r`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readViewProjection(parsed, view, view); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(column, before) || len(view.Columns) != 2 || view.Columns[1].Name != "DisplayName" || view.Columns[1].Source != "NAME" {
		t.Fatalf("unrelated named metadata stolen: %+v", view.Columns)
	}
}

func TestWildcardRenameMetadataSourceLessPhysicalRecord(t *testing.T) {
	column := &spec.Column{Name: "NAME", ExplicitType: true, Type: spec.TypeRef{Name: "string"}, Tag: `json:"display"`}
	view := &spec.View{Name: "Records", Namespace: "r", Columns: []*spec.Column{column}}
	parsed, err := sqlparser.ParseQuery(`SELECT r.*,r.NAME AS DisplayName FROM (SELECT NAME FROM RECORDS) r`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readViewProjection(parsed, view, view); err != nil {
		t.Fatal(err)
	}
	if len(view.Columns) != 1 || view.Columns[0] != column || column.Name != "DisplayName" || column.Source != "NAME" || !column.ExplicitType || column.Type.Name != "string" || reflect.StructTag(column.Tag).Get("json") != "display" || reflect.StructTag(column.Tag).Get("sqlx") != "NAME" {
		t.Fatalf("source-less physical metadata not reused: %+v", view.Columns)
	}
}
