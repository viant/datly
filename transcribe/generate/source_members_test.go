package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func sourceMemberAST(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, parser.ParseComments)
	require.NoError(t, err)
	return file
}
func TestSourceMemberAdmission(t *testing.T) {
	generated := sourceMemberAST(t, "package sample\ntype Request struct { Token *string; Body *Payload }; type Payload struct { Data []*Record }")
	text := "package sample\ntype Request struct { Token *string; Body *Payload; Partial bool; origin bool }; type Payload struct { Data []*Record; records []*Record; InputHas *Presence; projection *projectionState }"
	selected := []spec.SourceMember{{Owner: "Request", Name: "Partial"}, {Owner: "Request", Name: "origin"}, {Owner: "Payload", Name: "records"}, {Owner: "Payload", Name: "InputHas"}, {Owner: "Payload", Name: "projection"}}
	original := sourceMemberAST(t, text)
	result, err := retainSourceMemberAST(original, generated, selected, true)
	require.NoError(t, err)
	rendered, err := (xshape.SourceParser{}).FormatFile(result)
	require.NoError(t, err)
	require.Contains(t, string(rendered), "Partial")
	for name, source := range map[string]string{
		"stale":           strings.Replace(text, "origin bool", "other bool", 1),
		"injected marker": strings.Replace(text, "Token *string", "Has *Presence; Token *string", 1),
		"unknown":         strings.Replace(text, "Token *string", "Extra string; Token *string", 1),
		"authority drift": strings.Replace(text, "Token *string", "Token string", 1),
		"tag drift":       strings.Replace(text, "Token *string", "Token *string `json:\"token\"`", 1),
		"order":           strings.Replace(text, "Token *string; Body *Payload", "Body *Payload; Token *string", 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := retainSourceMemberAST(sourceMemberAST(t, source), generated, selected, true)
			require.Error(t, err)
		})
	}
	_, err = retainSourceMemberAST(original, generated, append(selected, selected[0]), true)
	require.ErrorContains(t, err, "duplicate")
	_, err = retainSourceMemberAST(original, generated, append(selected, spec.SourceMember{Owner: "Request", Name: "Token"}), true)
	require.ErrorContains(t, err, "generated ownership")
}
func TestRetainSourceMembersRegeneration(t *testing.T) {
	original := sourceMemberAST(t, "package sample\ntype Payload struct { Data []*Record; records []*Record; obsolete string; projection *projectionState `json:\"-\"` }")
	generated := sourceMemberAST(t, "package sample\ntype Payload struct { Data []*Record; Version *int }")
	selected := []spec.SourceMember{{Owner: "Payload", Name: "records"}, {Owner: "Payload", Name: "projection"}}
	first, err := retainSourceMemberAST(original, generated, selected, false)
	require.NoError(t, err)
	firstSource, err := (xshape.SourceParser{}).FormatFile(first)
	require.NoError(t, err)
	require.NotContains(t, string(firstSource), "obsolete")
	require.Contains(t, string(firstSource), "Version")
	require.Contains(t, string(firstSource), "`json:\"-\"`")
	second, err := retainSourceMemberAST(first, generated, selected, false)
	require.NoError(t, err)
	secondSource, err := (xshape.SourceParser{}).FormatFile(second)
	require.NoError(t, err)
	require.Equal(t, string(firstSource), string(secondSource))
	// Shared AST inputs were not modified by either rendering.
	fields, err := sourceStructs(generated)
	require.NoError(t, err)
	require.Len(t, fields["Payload"].Fields.List, 2)
}

func TestSourceMemberFunctionBytes(t *testing.T) {
	source := `package sample
 type Payload struct { Data []*Record; internal bool }
 // NewPayload preserves origin.
 //go:noinline
 func NewPayload() *Payload {
  // This inline comment must survive generation.
  return &Payload{internal: true}
 }
 `
	file := sourceMemberAST(t, source)
	functions, err := sourceMemberFunctions(SourceMemberFile{Source: source, File: file})
	require.NoError(t, err)
	require.Contains(t, functions, "//go:noinline")
	require.Contains(t, functions, "// This inline comment must survive generation.")
	require.Contains(t, functions, "return &Payload{internal: true}")
	for _, extra := range []string{"const local = true", "var local bool", "type local struct{}"} {
		_, err := retainSourceMemberAST(sourceMemberAST(t, source+"\n"+extra), sourceMemberAST(t, "package sample\ntype Payload struct { Data []*Record }"), []spec.SourceMember{{Owner: "Payload", Name: "internal"}}, true)
		require.ErrorContains(t, err, "unsupported application declaration")
	}
}

func TestSourceMemberCanonicalContractPreservesOriginalAST(t *testing.T) {
	source := "package sample\nimport legacy \"example.com/wire\"\ntype Request struct{Token *legacy.Token `codec:\"Decode\" parameter:\",kind=header,in=Token,required=true,errorCode=401\"`; private int}"
	generated := "package sample\nimport fresh \"example.com/wire\"\ntype Request struct{Token *fresh.Token `parameter:\"Token,kind=header,in=Token,dataType=*fresh.Token,errorCode=401,required=true\" codec:\"Decode\"`}"
	signature := func(file *ast.File, field *ast.Field) (string, error) {
		return sourceFieldSemanticSignature(file, field, "example.test/sample", nil)
	}
	selected := []spec.SourceMember{{Owner: "Request", Name: "private"}}
	old := sourceMemberAST(t, source)
	out, err := retainSourceMemberAST(old, sourceMemberAST(t, generated), selected, true, signature)
	require.NoError(t, err)
	got, _ := sourceStructs(out)
	prior, _ := sourceStructs(old)
	require.Same(t, prior["Request"].Fields.List[0], got["Request"].Fields.List[0], "equivalent field must retain original source AST")
	for _, tc := range []struct{ name, from, to string }{{"required", "required=true", "required=false"}, {"error status", "errorCode=401", "errorCode=403"}, {"codec", "Decode", "Different"}, {"provider type", "dataType=*fresh.Token", "dataType=string"}} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := retainSourceMemberAST(old, sourceMemberAST(t, strings.Replace(generated, tc.from, tc.to, 1)), selected, true, signature)
			require.ErrorContains(t, err, "differs from independently generated authority")
		})
	}
}
