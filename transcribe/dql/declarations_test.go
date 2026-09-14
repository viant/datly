package dql

import (
	"strings"
	"testing"
)

func TestParseDeclarationSQLRejectsUnclosedComment(t *testing.T) {
	parser := declarationOptionParser{paramName: "Broken", tail: "/* SELECT 1"}
	_, err := parser.parse()
	if err == nil || !strings.Contains(err.Error(), "unclosed declaration SQL comment") {
		t.Fatalf("declarationOptionParser.parse() error = %v", err)
	}
}

func TestDeclarationOptionParserIgnoresCommentDelimitersInsideOptionValues(t *testing.T) {
	parser := declarationOptionParser{
		paramName: "ID",
		tail:      ".WithErrorMessage('bad /* marker */ value') /* SELECT 1 */",
	}
	actual, err := parser.parse()
	if err != nil {
		t.Fatalf("declarationOptionParser.parse() error = %v", err)
	}
	if actual.errorMessage != "bad /* marker */ value" || actual.declarationSQL != "SELECT 1" {
		t.Fatalf("unexpected options: %+v", actual)
	}
}

func TestDeclarationOptionParserRejectsSyntaxAfterSQLComment(t *testing.T) {
	parser := declarationOptionParser{paramName: "ID", tail: "/* SELECT 1 */ trailing"}
	_, err := parser.parse()
	if err == nil || !strings.Contains(err.Error(), "unsupported syntax after declaration SQL") {
		t.Fatalf("declarationOptionParser.parse() error = %v", err)
	}
}

func TestInferImplicitDeclarationKindUsesParsedSource(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    string
		wantErr bool
	}{
		{name: "StructQL path", sql: "SELECT ID FROM `/Events`", want: "param"},
		{name: "ordinary table", sql: "SELECT ID FROM EVENTS", want: "view"},
		{name: "path text in literal", sql: "SELECT 'FROM `/Events`' AS Message", want: "view"},
		{name: "malformed", sql: "{not-json} SELECT 1", wantErr: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := inferImplicitDeclarationKind(testCase.sql)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("inferImplicitDeclarationKind() = %q, nil", actual)
				}
				return
			}
			if err != nil || actual != testCase.want {
				t.Fatalf("inferImplicitDeclarationKind() = %q, %v; want %q", actual, err, testCase.want)
			}
		})
	}
}

func TestPrepareSourceRejectsDuplicateCanonicalDefinitionsAtSecondDirective(t *testing.T) {
	second := `#define($_ = $ID<int>(query/id).Required())`
	source := `#define($_ = $ID<int>(query/id).Optional())
` + second + `
SELECT 1`
	prepared := PrepareSource(source)
	if len(prepared.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v", prepared.Diagnostics)
	}
	diagnostic := prepared.Diagnostics[0]
	if diagnostic.Offset != strings.Index(source, second) || diagnostic.End != strings.Index(source, second)+len(second) ||
		!strings.Contains(diagnostic.Message, "parameter ID is defined more than once") {
		t.Fatalf("duplicate definition diagnostic = %#v", diagnostic)
	}
}

func TestPrepareSourcePreservesDeclarationAuthorityRules(t *testing.T) {
	tests := []struct {
		name         string
		declarations string
		wantRequired bool
	}{
		{
			name: "set remains first wins",
			declarations: `#set($_ = $ID<int>(query/id).Optional())
#set($_ = $ID<int>(query/id).Required())`,
			wantRequired: false,
		},
		{
			name: "define replaces set",
			declarations: `#set($_ = $ID<int>(query/id).Optional())
#define($_ = $ID<int>(query/id).Required())`,
			wantRequired: true,
		},
		{
			name: "set cannot replace define",
			declarations: `#define($_ = $ID<int>(query/id).Required())
#set($_ = $ID<int>(query/id).Optional())`,
			wantRequired: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared := PrepareSource(test.declarations + "\nSELECT 1")
			if err := prepared.Err(); err != nil {
				t.Fatalf("PrepareSource() error = %v", err)
			}
			if len(prepared.Directives.Params) != 1 || prepared.Directives.Params[0].Required == nil ||
				*prepared.Directives.Params[0].Required != test.wantRequired {
				t.Fatalf("parameters = %#v", prepared.Directives.Params)
			}
		})
	}
}
