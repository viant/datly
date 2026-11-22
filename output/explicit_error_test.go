package output

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type explicitErrorRow struct {
	Value     string `json:"public"`
	Secret    string `json:"aliasForSecret"`
	MCPSecret string `json:"httpOnly" mcp:"-"`
}

type explicitErrorPayload struct {
	Rows []explicitErrorRow `json:"data"`
}

type explicitErrorSuccess struct {
	Rows   []explicitErrorRow `json:"successData"`
	Result string
}

type explicitErrorOther struct {
	Message string
}

func TestExplicitErrorDisclosure(t *testing.T) {
	canonical := reflect.TypeFor[explicitErrorPayload]()
	body := &explicitErrorPayload{Rows: []explicitErrorRow{{Value: "public", Secret: "never disclose", MCPSecret: "HTTP only"}}}
	component := &spec.Component{
		ErrorOutputs: []spec.ErrorOutput{{Type: canonical, Presentation: spec.ErrorOutputLegacyText}},
		Settings:     &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Rows.Secret"}}},
	}
	plan, err := (Compiler{}).Compile(CompileInput{Component: component, Type: canonical})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "ErrorDisclosure"}}

	ctx := dexec.CaptureContracts(context.Background())
	dexec.PublishContract(ctx, target, &dexec.EffectiveContract{Output: canonical})
	http, mode, matched, err := plan.EncodeExplicitError(ctx, target, body, false)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, spec.ErrorOutputLegacyText, mode)
	require.JSONEq(t, `{"data":[{"public":"public","httpOnly":"HTTP only"}]}`, string(http.Data))
	tool, _, matched, err := plan.EncodeExplicitError(ctx, target, body, true)
	require.NoError(t, err)
	require.True(t, matched)
	require.JSONEq(t, `{"data":[{"public":"public"}]}`, string(tool.Data))

	walk, err := xshape.CompileRewrite(canonical)
	require.NoError(t, err)
	view, err := walk.RewriteRepresentation(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		field := *f
		if parent == reflect.TypeFor[explicitErrorRow]() && f.Name == "Value" {
			field.Tag = (&xcontract.Field{Name: f.Name, Tag: f.Tag}).Hide().Tag
		}
		return &field, nil
	})
	require.NoError(t, err)
	dexec.PublishContract(ctx, target, &dexec.EffectiveContract{Output: canonical, OutputRepresentation: view})
	http, _, matched, err = plan.EncodeExplicitError(ctx, target, body, false)
	require.NoError(t, err)
	require.True(t, matched)
	require.JSONEq(t, `{"data":[{"httpOnly":"HTTP only"}]}`, string(http.Data))
	tool, _, matched, err = plan.EncodeExplicitError(ctx, target, body, true)
	require.NoError(t, err)
	require.True(t, matched)
	require.JSONEq(t, `{"data":[{}]}`, string(tool.Data))

	otherType := reflect.TypeFor[explicitErrorOther]()
	walk, err = xshape.CompileRewrite(otherType)
	require.NoError(t, err)
	view, err = walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		field := *f
		field.Tag = (&xcontract.Field{Name: f.Name, Tag: f.Tag}).Hide().Tag
		return &field, nil
	})
	require.NoError(t, err)
	dexec.PublishContract(ctx, target, &dexec.EffectiveContract{Output: otherType, OutputRepresentation: view})
	for _, mcp := range []bool{false, true} {
		encoded, _, matched, err := plan.EncodeExplicitError(ctx, target, body, mcp)
		require.Error(t, err)
		require.True(t, matched)
		require.Empty(t, encoded.Data)
	}

	_, _, matched, err = plan.EncodeExplicitError(ctx, target, json.RawMessage("null"), false)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestExplicitErrorSharesCallerProjectionAcrossEnvelopes(t *testing.T) {
	bodyType := reflect.TypeFor[explicitErrorPayload]()
	success := reflect.TypeFor[explicitErrorSuccess]()
	component := &spec.Component{ErrorOutputs: []spec.ErrorOutput{{Type: bodyType, Presentation: spec.ErrorOutputLegacyText}}}
	plan, err := (Compiler{}).Compile(CompileInput{Component: component, Type: success})
	require.NoError(t, err)
	walker, err := xshape.CompileRewrite(success)
	require.NoError(t, err)
	metadata, err := walker.RewriteRepresentation(func(_ string, owner reflect.Type, field *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if owner != reflect.TypeFor[explicitErrorRow]() || field.Name != "Secret" {
			return nil, nil
		}
		edited := *field
		edited.Tag = (&xcontract.Field{Name: field.Name, Tag: field.Tag}).Hide().Tag
		return &edited, nil
	})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "SharedErrorProjection"}}
	ctx := dexec.CaptureContracts(context.Background())
	dexec.PublishContract(ctx, target, &dexec.EffectiveContract{Output: success, OutputRepresentation: metadata})
	body := explicitErrorPayload{Rows: []explicitErrorRow{{Value: "public", Secret: "never disclose", MCPSecret: "HTTP only"}}}
	http, _, matched, err := plan.EncodeExplicitError(ctx, target, body, false)
	require.NoError(t, err)
	require.True(t, matched)
	require.JSONEq(t, `{"data":[{"public":"public","httpOnly":"HTTP only"}]}`, string(http.Data))
	tool, _, matched, err := plan.EncodeExplicitError(ctx, target, body, true)
	require.NoError(t, err)
	require.True(t, matched)
	require.JSONEq(t, `{"data":[{"public":"public"}]}`, string(tool.Data))
}

func TestExplicitErrorDeclarationsRejectConflicts(t *testing.T) {
	typ := reflect.TypeFor[explicitErrorOther]()
	component := &spec.Component{ErrorOutputs: []spec.ErrorOutput{
		{Type: typ}, {Type: reflect.PointerTo(typ), Presentation: spec.ErrorOutputLegacyText},
	}}
	_, err := (Compiler{}).Compile(CompileInput{Component: component, Type: typ})
	require.ErrorContains(t, err, "duplicate explicit error output")
}
