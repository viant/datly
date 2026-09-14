package statement

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantKinds  []Kind
		operations []string
	}{
		{name: "read", source: "SELECT id FROM orders", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "SQL exec", source: "INSERT INTO orders(id) VALUES (1);\nUPDATE orders SET name = 'x'", wantKinds: []Kind{KindExec, KindExec}, operations: []string{"INSERT", "UPDATE"}},
		{name: "insert service", source: `$dml.Insert("ORDERS", $rec)`, wantKinds: []Kind{KindService}, operations: []string{"Insert"}},
		{name: "update service", source: `$dml.Update("ORDERS", $rec)`, wantKinds: []Kind{KindService}, operations: []string{"Update"}},
		{name: "delete service", source: `$dml.Delete("ORDERS", $rec)`, wantKinds: []Kind{KindService}, operations: []string{"Delete"}},
		{name: "execute service", source: `$dml.Execute("DELETE FROM ORDERS WHERE ID = ?", $ID)`, wantKinds: []Kind{KindService}, operations: []string{"Execute"}},
		{name: "SQL capability is not accepted", source: `$sql.Insert("ORDERS", $rec)`, wantKinds: []Kind{KindUnknown}, operations: []string{""}},
		{name: "nop", source: `$Nop($Unsafe.ID)`, wantKinds: []Kind{KindExec}, operations: []string{"Nop"}},
		{name: "unknown", source: `$foo.Bar($value)`, wantKinds: []Kind{KindUnknown}, operations: []string{""}},
		{name: "quoted keywords", source: "-- insert into ignored\nSELECT 'update ignored' AS text FROM orders", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "nested select", source: "SELECT root.* FROM (SELECT * FROM child) root", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "Velty preamble", source: "#set($criteria = $ID)\nSELECT * FROM orders WHERE id = $criteria", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "unsupported SQL", source: "CREATE TABLE events(id INT)", wantKinds: []Kind{KindUnknown}, operations: []string{"CREATE"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Parse(test.source)
			if len(actual) != len(test.wantKinds) {
				t.Fatalf("statements = %#v", actual)
			}
			for index := range actual {
				if actual[index].Kind != test.wantKinds[index] || actual[index].Operation != test.operations[index] {
					t.Fatalf("statement %d = %#v", index, actual[index])
				}
				if actual[index].Start < 0 || actual[index].End <= actual[index].Start || actual[index].End > len(test.source) {
					t.Fatalf("invalid statement span: %#v", actual[index])
				}
				if actual[index].SQLEnd < actual[index].SQLStart || actual[index].SQLEnd > actual[index].End {
					t.Fatalf("invalid SQL span: %#v", actual[index])
				}
				if !actual[index].TemplateBalanced {
					t.Fatalf("unexpected unbalanced template: %#v", actual[index])
				}
			}
		})
	}
}

func TestParseReportsUnclosedPreSQLTemplateBlock(t *testing.T) {
	for _, source := range []string{
		"#if($Enabled)\nSELECT * FROM orders",
		"#foreach($tenant in $Tenants)\nSELECT * FROM orders WHERE tenant_id = $tenant.ID",
	} {
		actual := Parse(source)
		if len(actual) != 1 || actual[0].Kind != KindRead || actual[0].TemplateBalanced || actual[0].SQLEnd != actual[0].End {
			t.Fatalf("source %q: statements = %#v", source, actual)
		}
	}
}

func TestParseSeparatesBalancedVeltyWrapperFromSQL(t *testing.T) {
	source := `#if($Enabled)
SELECT * FROM orders
WHERE id IN (#foreach($id in $IDs)$id#if($foreach.HasNext),#end#end)
#end`
	actual := Parse(source)
	if len(actual) != 1 || actual[0].Kind != KindRead {
		t.Fatalf("statements = %#v", actual)
	}
	item := actual[0]
	if prefix := strings.TrimSpace(source[item.Start:item.SQLStart]); prefix != "#if($Enabled)" {
		t.Fatalf("prefix = %q", prefix)
	}
	SQL := strings.TrimSpace(source[item.SQLStart:item.SQLEnd])
	if !strings.HasPrefix(SQL, "SELECT * FROM orders") || !strings.Contains(SQL, "#foreach($id in $IDs)") || strings.HasSuffix(SQL, "#end\n#end") {
		t.Fatalf("SQL = %q", SQL)
	}
	if suffix := strings.TrimSpace(source[item.SQLEnd:item.End]); suffix != "#end" {
		t.Fatalf("suffix = %q", suffix)
	}
}

func TestParsePreservesCTEAfterTemplatePrefix(t *testing.T) {
	for _, source := range []string{
		"#set($criteria = $ID)\nWITH active AS (SELECT id FROM users) SELECT * FROM active",
		"#if($Enabled)\nWITH active AS (SELECT id FROM users) SELECT * FROM active\n#end",
	} {
		actual := Parse(source)
		if len(actual) != 1 || actual[0].Kind != KindRead || !actual[0].TemplateBalanced {
			t.Fatalf("source %q: statements = %#v", source, actual)
		}
		if SQL := strings.TrimSpace(source[actual[0].SQLStart:actual[0].SQLEnd]); !strings.HasPrefix(SQL, "WITH active AS") {
			t.Fatalf("source %q: SQL = %q", source, SQL)
		}
	}
}

func TestParsePreservesWholeStatementSpans(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{name: "CTE", source: "WITH active AS (SELECT id FROM users) SELECT id FROM active"},
		{name: "Velty preamble", source: "#set($criteria = $ID)\nSELECT * FROM orders WHERE id = $criteria"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Parse(test.source)
			if len(actual) != 1 || actual[0].Kind != KindRead {
				t.Fatalf("statements = %#v", actual)
			}
			if got := test.source[actual[0].Start:actual[0].End]; got != test.source {
				t.Fatalf("statement span = %q", got)
			}
			if got := test.source[actual[0].OperationStart : actual[0].OperationStart+len(actual[0].Operation)]; got != "SELECT" {
				t.Fatalf("operation span = %q", got)
			}
			if test.name == "CTE" && actual[0].SQLStart != 0 {
				t.Fatalf("CTE SQL start = %d", actual[0].SQLStart)
			}
			if test.name == "Velty preamble" && actual[0].SQLStart != actual[0].OperationStart {
				t.Fatalf("Velty SQL start = %d, operation = %d", actual[0].SQLStart, actual[0].OperationStart)
			}
		})
	}
}

func TestParseDoesNotHideUnsupportedOperationAfterRead(t *testing.T) {
	source := "SELECT 1;\nCREATE TABLE events(id INT)"
	actual := Parse(source)
	if len(actual) != 2 || actual[0].Kind != KindRead || actual[1].Kind != KindUnknown || actual[1].Operation != "CREATE" {
		t.Fatalf("statements = %#v", actual)
	}
	if got := strings.TrimSpace(source[actual[0].Start:actual[0].End]); got != "SELECT 1" {
		t.Fatalf("read span = %q", got)
	}
	if got := source[actual[1].Start:actual[1].End]; got != "CREATE TABLE events(id INT)" {
		t.Fatalf("unknown span = %q", got)
	}
	classification := actual.Classify()
	if !classification.HasRead || !classification.HasUnknown {
		t.Fatalf("classification = %+v", classification)
	}
}

func TestParseBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantKinds  []Kind
		operations []string
	}{
		{name: "semicolon without whitespace", source: "SELECT 1;UPDATE events SET id = 2", wantKinds: []Kind{KindRead, KindExec}, operations: []string{"SELECT", "UPDATE"}},
		{name: "transaction script", source: "BEGIN;\nUPDATE events SET id = 2;\nCOMMIT", wantKinds: []Kind{KindExec, KindExec, KindUnknown}, operations: []string{"BEGIN", "UPDATE", "COMMIT"}},
		{name: "block comment", source: "/* UPDATE ignored */ SELECT 1", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "insert select", source: "INSERT INTO target(id) SELECT id FROM source", wantKinds: []Kind{KindExec}, operations: []string{"INSERT"}},
		{name: "union select", source: "SELECT id FROM first UNION SELECT id FROM second", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "conflict update", source: "INSERT INTO target(id) VALUES (1) ON CONFLICT(id) DO UPDATE SET id = excluded.id", wantKinds: []Kind{KindExec}, operations: []string{"INSERT"}},
		{name: "dialect with clause", source: "SELECT * FROM target WITH (NOLOCK)", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "quoted semicolon", source: "SELECT ';' AS delimiter", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "comment semicolon", source: "/* ; */ SELECT 1", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "nested semicolon", source: "SELECT fn(';')", wantKinds: []Kind{KindRead}, operations: []string{"SELECT"}},
		{name: "operation suffix", source: "myselect value", wantKinds: []Kind{KindUnknown}, operations: []string{""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := Parse(test.source)
			if len(actual) != len(test.wantKinds) {
				t.Fatalf("statements = %#v", actual)
			}
			for index := range actual {
				if actual[index].Kind != test.wantKinds[index] || actual[index].Operation != test.operations[index] {
					t.Fatalf("statement %d = %#v", index, actual[index])
				}
			}
		})
	}
}

func TestClassification(t *testing.T) {
	actual := Parse("SELECT id FROM orders;\nUPDATE orders SET id = 2").Classify()
	if !actual.HasRead || !actual.HasExec || actual.HasUnknown || actual.HasService {
		t.Fatalf("classification = %+v", actual)
	}
	unknown := Parse("$foo.Bar($x)").Classify()
	if !unknown.HasUnknown || unknown.HasRead || unknown.HasExec || unknown.HasService {
		t.Fatalf("unknown classification = %+v", unknown)
	}
}

func TestParsePreservesMatchedTokenAcrossWhitespace(t *testing.T) {
	actual := Parse("INSERT INTO orders(id) VALUES (1)")
	if len(actual) != 1 || actual[0].Kind != KindExec || actual[0].Operation != "INSERT" {
		t.Fatalf("statements = %#v", actual)
	}
}

func TestParseIgnoresEmptyAndCommentOnlySegments(t *testing.T) {
	actual := Parse("; /* comment only */; -- another comment\n; SELECT 1;;")
	if len(actual) != 1 || actual[0].Kind != KindRead || actual[0].Operation != "SELECT" {
		t.Fatalf("statements = %#v", actual)
	}
}
