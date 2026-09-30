package compile

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReaderPreservesExecutableSourceStatements(t *testing.T) {
	cases := []struct {
		name, sql string
		want      []string
	}{
		{"direct conditional predicate", `SELECT r.id,use_connector(r,'main') FROM records r WHERE 1=1
#if($Scoped)
AND r.owner_id=$Owner
#else
AND r.public=1
#end
ORDER BY r.id`, []string{"#if($Scoped)\nAND r.owner_id=$Owner\n#else\nAND r.public=1\n#end"}},
		{"named projection", `SELECT records.id AS Identifier FROM (
 SELECT r.id FROM records r WHERE 1=1
 #if($LockRows) ${View.ForUpdate()} #end
 ) records`, []string{`#if($LockRows) ${View.ForUpdate()} #end`}},
		{"direct root suffix", `SELECT r.id,use_connector(r,'main') FROM records r ORDER BY r.id
 #if($Locked) ${View.ForUpdate()} #end`, []string{`#if($Locked) ${View.ForUpdate()} #end`}},
		{"balanced branches", `SELECT records.id AS Identifier FROM (
 SELECT r.id FROM records r WHERE 1=1
 #if($Scoped)
 AND r.owner_id=$Owner
 #else
 AND r.public=1
 #end
 ) records`, []string{"#if($Scoped)\n AND r.owner_id=$Owner\n #else\n AND r.public=1\n #end"}},
		{"loop fragment", `SELECT records.id AS Identifier FROM (
 SELECT r.id FROM records r WHERE 1=1
 #foreach($id in $IDs)
 AND r.id<>$id
 #end
 ) records`, []string{"#foreach($id in $IDs)\n AND r.id<>$id\n #end"}},
		{"distinct repeated source bodies", `SELECT a.id AS First,b.id AS Second FROM (
 SELECT r.id FROM records r WHERE 1=1
 #if($FirstLock) ${View.ForUpdate()} #end
 ) a JOIN (
 SELECT r.id FROM records r WHERE 1=1
 #if($SecondLock) ${View.ForUpdate()} #end
 ) b ON b.id=a.id`, []string{`#if($FirstLock) ${View.ForUpdate()} #end`, `#if($SecondLock) ${View.ForUpdate()} #end`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, err := NewReader().Compile(ReadInput{SQL: tc.sql, View: &spec.View{Name: "reader", Source: &spec.ViewSource{SQL: tc.sql}}})
			if err != nil {
				t.Fatal(err)
			}
			text := root.Source.SQL
			for _, rel := range root.Relations {
				text += "\n" + rel.View.Source.SQL
			}
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Fatalf("lost original statement %q:\n%s", want, text)
				}
			}
			if strings.Contains(text, "__datly_read_template_") {
				t.Fatalf("analysis source leaked into executable SQL: %s", text)
			}
			if tc.name == "distinct repeated source bodies" {
				if strings.Contains(root.Source.SQL, "SecondLock") || strings.Contains(root.Relations[0].View.Source.SQL, "FirstLock") {
					t.Fatalf("source identities were confused: %s", text)
				}
			}
		})
	}
}
func TestReaderRejectsMalformedExecutableSourceStatement(t *testing.T) {
	sql := "SELECT r.id AS Identifier FROM (SELECT id FROM records\n#if($Locked) broken) r"
	if _, err := NewReader().Compile(ReadInput{SQL: sql, View: &spec.View{Name: "r", Source: &spec.ViewSource{SQL: sql}}}); err == nil {
		t.Fatal("malformed executable statement was silently removed")
	}
}

func TestReaderTemplateBranchesCannotHideNestedMetadata(t *testing.T) {
	sql := `SELECT records.id AS Identifier FROM (
 SELECT r.id
 #if($Enabled)
 ,tag(r.id,'json:"hidden"')
 #end
 FROM records r
 ) records`
	_, err := NewReader().Compile(ReadInput{SQL: sql, View: &spec.View{Name: "reader", Source: &spec.ViewSource{SQL: sql}}})
	if err == nil || !strings.Contains(err.Error(), CodeViewDirective) {
		t.Fatalf("branch hid invalid nested metadata: %v", err)
	}
}

func TestReadPhysicalSourceRangeSkipsTemplateSelectors(t *testing.T) {
	for _, projection := range []string{"$FROM", "${FROM}", "$helper.Value($FROM)", "(SELECT x.id FROM other x)", "'FROM'", "\"FROM\""} {
		sql := "SELECT " + projection + ",r.id FROM records r WHERE r.id=$ID"
		offset := topLevelReadFrom(sql)
		if offset < 0 || sql[offset:] != "FROM records r WHERE r.id=$ID" {
			t.Fatalf("physical source range includes projection in %s: %d", sql, offset)
		}
	}
}
