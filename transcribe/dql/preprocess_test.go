package dql

import (
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/dql/statement"
)

func TestPrepareSourceMasksDirectivesAndExtractsTypeContext(t *testing.T) {
	source := `#package('example.com/demo')
#import('model','example.com/demo/model')
#setting($_ = $route('/v1/users','GET'))
#define($_ = $ID<int>(query/id))
SELECT id FROM users WHERE id = :ID`
	prepared := PrepareSource(source)
	if err := prepared.Err(); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if prepared.TypeContext == nil || prepared.TypeContext.DefaultPackage != "example.com/demo" {
		t.Fatalf("unexpected type context: %#v", prepared.TypeContext)
	}
	if len(prepared.TypeContext.Imports) != 1 || prepared.TypeContext.Imports[0].Alias != "model" {
		t.Fatalf("unexpected imports: %#v", prepared.TypeContext.Imports)
	}
	if strings.Contains(prepared.SQL, "#define") || strings.Contains(prepared.SQL, "#setting") || strings.Contains(prepared.SQL, "#import") {
		t.Fatalf("directives leaked into SQL: %q", prepared.SQL)
	}
	if !strings.Contains(prepared.SQL, "SELECT id FROM users") {
		t.Fatalf("SQL body was lost: %q", prepared.SQL)
	}
	if strings.Count(prepared.DirectSQL, "\n") != strings.Count(source, "\n") {
		t.Fatalf("preprocessing must preserve authored line count")
	}
	if prepared.Directives == nil || prepared.Directives.Route == nil {
		t.Fatalf("normalized directives are missing: %#v", prepared.Directives)
	}
	if prepared.Directives.Route.URI != "/v1/users" || len(prepared.Directives.Route.Methods) != 1 || prepared.Directives.Route.Methods[0] != "GET" {
		t.Fatalf("unexpected normalized route: %#v", prepared.Directives.Route)
	}
	if len(prepared.Directives.Params) != 1 || prepared.Directives.Params[0].Name != "ID" || prepared.Directives.Params[0].TypeExpr != "int" {
		t.Fatalf("unexpected normalized params: %#v", prepared.Directives.Params)
	}
}

func TestPrepareSourceBuildsStatementPlanFromExecutableSQL(t *testing.T) {
	source := `#setting($_ = $route('/events', 'GET'))
	SELECT id FROM events;
UPDATE events SET active = 1`
	prepared := PrepareSource(source)
	if err := prepared.Err(); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if len(prepared.Statements) != 2 {
		t.Fatalf("statements = %#v", prepared.Statements)
	}
	if prepared.Statements[0].Kind != statement.KindRead || prepared.Statements[0].Operation != "SELECT" {
		t.Fatalf("read statement = %#v", prepared.Statements[0])
	}
	if prepared.Statements[1].Kind != statement.KindExec || prepared.Statements[1].Operation != "UPDATE" {
		t.Fatalf("exec statement = %#v", prepared.Statements[1])
	}
	if got := prepared.SQL[prepared.Statements[1].Start:prepared.Statements[1].End]; !strings.HasPrefix(got, "UPDATE") {
		t.Fatalf("exec span = %q", got)
	}
}

func TestPrepareSourceInfersRoutePathParams(t *testing.T) {
	prepared := PrepareSource(`#setting($_ = $route('/teams/{teamID}/members/{memberID}/{teamID}', 'GET'))
SELECT 1`)
	if err := prepared.Err(); err != nil {
		t.Fatalf("PrepareSource() error = %v", err)
	}
	params := prepared.Directives.Params
	if len(params) != 2 || params[0].Name != "teamID" || params[1].Name != "memberID" {
		t.Fatalf("Params = %+v", params)
	}
	for _, param := range params {
		if param.Source.Kind != "path" || param.Source.Name != param.Name || param.TypeExpr != "string" || param.Cardinality != "One" {
			t.Fatalf("inferred param = %+v", param)
		}
	}
}

func TestPrepareSourceExplicitPathParamWinsOverInference(t *testing.T) {
	prepared := PrepareSource(`#setting($_ = $route('/vendors/{vendorID}', 'GET'))
#define($_ = $VendorID<int>(path/vendorID))
SELECT 1`)
	if err := prepared.Err(); err != nil {
		t.Fatalf("PrepareSource() error = %v", err)
	}
	params := prepared.Directives.Params
	if len(params) != 1 || params[0].Name != "VendorID" || params[0].TypeExpr != "int" || params[0].Declaration != "define" {
		t.Fatalf("Params = %+v", params)
	}
}

func TestPrepareSourceAllowsSameNameOutputWithInferredPathInput(t *testing.T) {
	prepared := PrepareSource(`#setting($_ = $route('/vendors/{vendorID}', 'GET'))
#define($_ = $vendorID<string>(output/status))
SELECT 1`)
	if err := prepared.Err(); err != nil {
		t.Fatalf("PrepareSource() error = %v", err)
	}
	params := prepared.Directives.Params
	if len(params) != 2 || params[0].Source.Kind != "output" || params[1].Source.Kind != "path" || params[1].Name != "vendorID" {
		t.Fatalf("Params = %+v", params)
	}
}

func TestPrepareSourceRejectsMalformedAndConflictingPathParams(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		message string
	}{
		{
			name: "malformed placeholder",
			source: `#setting($_ = $route('/vendors/{vendorID', 'GET'))
SELECT 1`,
			message: "complete segment",
		},
		{
			name: "conflicting source",
			source: `#setting($_ = $route('/vendors/{vendorID}', 'GET'))
#define($_ = $vendorID<string>(query/vendorID))
SELECT 1`,
			message: "conflicts with declared query source",
		},
		{
			name: "placeholder starts with digit",
			source: `#setting($_ = $route('/vendors/{1vendorID}', 'GET'))
SELECT 1`,
			message: "must start with a letter",
		},
		{
			name: "placeholder contains punctuation",
			source: `#setting($_ = $route('/vendors/{vendor-id}', 'GET'))
SELECT 1`,
			message: "must contain only letters, digits, or underscores",
		},
		{
			name: "path source has different case",
			source: `#setting($_ = $route('/vendors/{VendorID}', 'GET'))
#define($_ = $VendorID<int>(path/vendorID))
SELECT 1`,
			message: "must match declared path source \"vendorID\" exactly",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared := PrepareSource(test.source)
			if err := prepared.Err(); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("PrepareSource() error = %v", err)
			}
			if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Offset != 0 {
				t.Fatalf("Diagnostics = %+v", prepared.Diagnostics)
			}
		})
	}
}

func TestParsePreparedComponentSourceConsumesNormalizedDirectives(t *testing.T) {
	prepared := PrepareSource(`#setting($_ = $route('/events', 'POST'))
#setting($_ = $connector('analytics'))
#define($_ = $ID<int>(query/id))
SELECT id FROM events WHERE id = :ID`)
	if err := prepared.Err(); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	// Component assembly must not recover metadata by reparsing authored text.
	prepared.Original = "SELECT intentionally_invalid_after_prepare"
	component, err := ParsePreparedComponentSource("example.com/demo", "Events", prepared)
	if err != nil {
		t.Fatalf("parse prepared component: %v", err)
	}
	if len(component.Routes) != 1 || component.Routes[0].Path != "/events" || component.Routes[0].Method != "POST" {
		t.Fatalf("unexpected routes: %#v", component.Routes)
	}
	if component.Settings == nil || component.Settings.DefaultConnector != "analytics" {
		t.Fatalf("unexpected settings: %#v", component.Settings)
	}
	if len(component.Parameters) != 1 || component.Parameters[0].Name != "ID" {
		t.Fatalf("unexpected params: %#v", component.Parameters)
	}
}

func TestPrepareSourcePreservesAPIKeyRegardlessOfDirectiveOrder(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{
			name: "api key before route",
			source: `#setting($_ = $api_key('X-Key', 'secret'))
#setting($_ = $route('/events'))
SELECT 1`,
		},
		{
			name: "route before api key",
			source: `#setting($_ = $route('/events'))
#setting($_ = $api_key('X-Key', 'secret'))
SELECT 1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared := PrepareSource(test.source)
			if err := prepared.Err(); err != nil {
				t.Fatalf("prepare failed: %v", err)
			}
			route := prepared.Directives.Route
			if route == nil || route.URI != "/events" || len(route.Methods) != 1 || route.Methods[0] != "GET" || route.APIKeyHeader != "X-Key" || route.APIKeyValue != "secret" {
				t.Fatalf("unexpected route: %#v", route)
			}
		})
	}
}

func TestPrepareSourceReportsEarliestSemanticDirectiveError(t *testing.T) {
	source := `#setting($_ = $cache())
#setting($_ = $route('relative'))
SELECT 1`
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("expected one diagnostic, got %#v", prepared.Diagnostics)
	}
	diagnostic := prepared.Diagnostics[0]
	if diagnostic.Offset != 0 || !strings.Contains(diagnostic.Message, "cache") {
		t.Fatalf("expected first authored cache error, got %#v", diagnostic)
	}
}

func TestPrepareSourceInfersImportAliasFromPackageFolder(t *testing.T) {
	prepared := PrepareSource("#import('github.com/viant/xdatly/response')\nSELECT 1")
	if err := prepared.Err(); err != nil {
		t.Fatal(err)
	}
	if prepared.TypeContext == nil || len(prepared.TypeContext.Imports) != 1 || prepared.TypeContext.Imports[0].Alias != "response" || prepared.TypeContext.Imports[0].Package != "github.com/viant/xdatly/response" {
		t.Fatalf("imports = %#v", prepared.TypeContext)
	}
}

func TestPrepareSourceOrdersAllDiagnosticsByAuthoredOffset(t *testing.T) {
	source := `#import()
#setting($_ = $route('relative'))
SELECT 1`
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 2 {
		t.Fatalf("expected two diagnostics, got %#v", prepared.Diagnostics)
	}
	if prepared.Diagnostics[0].Code != DiagnosticInvalidImport || prepared.Diagnostics[0].Offset != 0 {
		t.Fatalf("expected import diagnostic first, got %#v", prepared.Diagnostics)
	}
	if prepared.Diagnostics[1].Code != DiagnosticParse || prepared.Diagnostics[1].Offset != strings.Index(source, "#setting") {
		t.Fatalf("expected route diagnostic second, got %#v", prepared.Diagnostics)
	}
	if err := prepared.Err(); err == nil || !strings.Contains(err.Error(), "#import") {
		t.Fatalf("expected earliest diagnostic from Err, got %v", err)
	}
}

func TestPrepareSourceRejectsMultipleRoutes(t *testing.T) {
	source := `#setting($_ = $route('/a'))
#setting($_ = $route('/b'))
SELECT 1`
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("expected one diagnostic, got %#v", prepared.Diagnostics)
	}
	diagnostic := prepared.Diagnostics[0]
	if diagnostic.Offset != strings.Index(source, "#setting($_ = $route('/b'))") || !strings.Contains(diagnostic.Message, "multiple routes") {
		t.Fatalf("unexpected duplicate-route diagnostic: %#v", diagnostic)
	}
}

func TestPrepareSourceRejectsInvalidAndDuplicateConstantsAtAuthoredDirective(t *testing.T) {
	tests := []struct {
		name    string
		setting string
		message string
	}{
		{name: "invalid identifier", setting: `#setting($_ = $const('vendor-name', 'VENDOR'))`, message: "Go identifier"},
		{name: "exact duplicate", setting: `#setting($_ = $const('Vendor', 'OTHER'))`, message: "ambiguous"},
		{name: "case duplicate", setting: `#setting($_ = $const('vendor', 'OTHER'))`, message: "ambiguous"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := `#setting($_ = $const('Vendor', 'VENDOR'))`
			if test.name == "invalid identifier" {
				first = `#setting($_ = $route('/vendors', 'GET'))`
			}
			source := first + "\n" + test.setting + "\nSELECT 1"
			prepared := PrepareSource(source)
			if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Offset != strings.Index(source, test.setting) ||
				!strings.Contains(prepared.Diagnostics[0].Message, test.message) {
				t.Fatalf("diagnostics = %#v", prepared.Diagnostics)
			}
		})
	}
}

func TestPrepareSourcePreservesHandlerSetLogic(t *testing.T) {
	source := `#setting($_ = $route('/v1/users','POST'))
#set($Output.ID = $Input.ID)`
	prepared := PrepareSource(source)
	if !strings.Contains(prepared.SQL, "#set($Output.ID = $Input.ID)") {
		t.Fatalf("handler logic was masked: %q", prepared.SQL)
	}
}

func TestPrepareSourceReportsInvalidImportAtAuthoredOffset(t *testing.T) {
	source := "  #import()\nSELECT 1"
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("expected one diagnostic, got %#v", prepared.Diagnostics)
	}
	diagnostic := prepared.Diagnostics[0]
	if diagnostic.Code != DiagnosticInvalidImport || diagnostic.Offset != 2 {
		t.Fatalf("unexpected diagnostic: %#v", diagnostic)
	}
}

func TestPrepareSourceRejectsMalformedMaskedDirectives(t *testing.T) {
	tests := []struct {
		name string
		line string
		code string
	}{
		{name: "setting", line: "#setting($_ = $connector('db')", code: DiagnosticInvalidSetting},
		{name: "define", line: "#define($_ = $ID<int>(query/id)", code: DiagnosticInvalidDefine},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared := PrepareSource(test.line + "\nSELECT 1")
			if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Code != test.code {
				t.Fatalf("diagnostics = %#v", prepared.Diagnostics)
			}
			if strings.Contains(prepared.SQL, test.line) {
				t.Fatalf("malformed directive leaked into SQL: %q", prepared.SQL)
			}
		})
	}
}

func TestPrepareSourceMasksMalformedMultilineDirectiveRemainder(t *testing.T) {
	source := "#setting($_ = $connector(\n  'db'\nSELECT 1"
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 || prepared.Diagnostics[0].Code != DiagnosticInvalidSetting {
		t.Fatalf("diagnostics = %#v", prepared.Diagnostics)
	}
	if prepared.SQL != "" {
		t.Fatalf("malformed directive continuation leaked into SQL: %q", prepared.SQL)
	}
	if strings.Count(prepared.DirectSQL, "\n") != strings.Count(source, "\n") {
		t.Fatalf("line count changed: %q", prepared.DirectSQL)
	}
}

func TestPrepareSourcePreservesCRLFAndUnicodeByteOffsets(t *testing.T) {
	source := "-- café\r\n\t#import()\r\nSELECT 1"
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v", prepared.Diagnostics)
	}
	want := strings.Index(source, "#import")
	if prepared.Diagnostics[0].Offset != want {
		t.Fatalf("offset = %d, want %d", prepared.Diagnostics[0].Offset, want)
	}
	if strings.Count(prepared.DirectSQL, "\r\n") != strings.Count(source, "\r\n") {
		t.Fatalf("CRLF count changed: %q", prepared.DirectSQL)
	}
}
