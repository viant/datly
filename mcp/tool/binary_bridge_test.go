package tool

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"mime/multipart"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func TestBinaryOriginalDescriptorForms(t *testing.T) {
	data := []byte{255, 254, 0}
	for name, value := range map[string]interface{}{
		"object":       map[string]interface{}{"data": base64.StdEncoding.EncodeToString(data)},
		"array":        []interface{}{map[string]interface{}{"data": base64.StdEncoding.EncodeToString(data)}},
		"string":       base64.StdEncoding.EncodeToString(data),
		"raw standard": base64.RawStdEncoding.EncodeToString(data),
		"raw url":      base64.RawURLEncoding.EncodeToString(data),
		"data uri":     "  data:application/octet-stream;base64," + base64.StdEncoding.EncodeToString(data) + "  ",
	} {
		t.Run(name, func(t *testing.T) {
			scope, err := originalBinaryPlan(t, false).Scope(map[string]interface{}{"Files": value})
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Form().Locate(nil).Value(context.Background(), reflect.TypeOf([]*multipart.FileHeader{}), "file")
			require.NoError(t, err)
			require.True(t, found)
			header := value.([]*multipart.FileHeader)[0]
			require.Equal(t, "blob", header.Filename)
			require.Equal(t, "application/octet-stream", header.Header.Get("Content-Type"))
			file, err := header.Open()
			require.NoError(t, err)
			defer file.Close()
			actual, err := io.ReadAll(file)
			require.NoError(t, err)
			require.Equal(t, data, actual)
		})
	}
	for _, args := range []map[string]interface{}{{"Files": "%%%"}, {"Files": "eA==", "Payload": map[string]interface{}{"UPLOADACTION": "x"}}, {"Files": "eA==", "Payload": map[string]interface{}{" ": "x"}}, {"Blob": "%%%"}, {"Blob": []interface{}{originalBlob("x"), originalBlob("y")}}} {
		hybrid := args["Blob"] != nil
		scope, err := originalBinaryPlan(t, hybrid).Scope(args)
		if scope != nil {
			defer scope.Close()
		}
		require.Error(t, err)
	}
}

type binaryContextKey struct{}
type binaryContextBody struct {
	Raw     string
	Context string
}

func (b *binaryContextBody) UnmarshalXLS(ctx context.Context, raw []byte) error {
	b.Raw = string(raw)
	b.Context, _ = ctx.Value(binaryContextKey{}).(string)
	return nil
}

type binaryComponentFunc func(context.Context, exec.ComponentRequest) (interface{}, error)

func (f binaryComponentFunc) InvokeComponent(ctx context.Context, r exec.ComponentRequest) (interface{}, error) {
	return f(ctx, r)
}

func TestBinaryHandlerOwnsSpillCleanup(t *testing.T) {
	// Cross Bindly's existing 32 MiB in-memory threshold, with no new size policy.
	data := bytes.Repeat([]byte("x"), 33<<20)
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "component failure"}[fail], func(t *testing.T) {
			var temporary string
			component := binaryComponentFunc(func(ctx context.Context, r exec.ComponentRequest) (interface{}, error) {
				for _, provider := range r.Providers {
					if provider.Kind() != "form" {
						continue
					}
					value, found, err := provider.Locate(nil).Value(ctx, reflect.TypeOf([]*multipart.FileHeader{}), "file")
					require.NoError(t, err)
					require.True(t, found)
					file, err := value.([]*multipart.FileHeader)[0].Open()
					require.NoError(t, err)
					temporary = file.(*os.File).Name()
					require.NoError(t, file.Close())
					_, err = os.Stat(temporary)
					require.NoError(t, err)
				}
				require.NotEmpty(t, temporary)
				if fail {
					return nil, errors.New("expected component failure")
				}
				return map[string]interface{}{"ok": true}, nil
			})
			plan := originalBinaryPlan(t, false)
			handler := NewHandler(plan, invocation.New(invocation.Config{Invoker: component}))
			request := &schema.CallToolRequest{}
			request.Params.Name = plan.Metadata().Name
			request.Params.Arguments = map[string]interface{}{"Files": base64.StdEncoding.EncodeToString(data)}
			_, protocolErr := handler.Handle(context.Background(), request)
			require.Nil(t, protocolErr)
			_, err := os.Stat(temporary)
			require.True(t, os.IsNotExist(err), "multipart spill remains: %v", err)
		})
	}
}

func TestBinaryHandlerXLSContextAndLowercaseBlob(t *testing.T) {
	type input struct {
		Body *binaryContextBody `mcp:"name=Body"`
	}
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/files", Name: "upload"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "files.upload"}, Contract: testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}}})})
	require.NoError(t, err)
	component := binaryComponentFunc(func(ctx context.Context, r exec.ComponentRequest) (interface{}, error) {
		for _, provider := range r.Providers {
			if provider.Kind() != "body" {
				continue
			}
			value, found, err := provider.Locate(nil).Value(ctx, reflect.TypeOf(&binaryContextBody{}), "body")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, &binaryContextBody{Raw: "workbook", Context: "caller"}, value)
		}
		return map[string]interface{}{"ok": true}, nil
	})
	request := &schema.CallToolRequest{}
	request.Params.Name = plan.Metadata().Name
	request.Params.Arguments = map[string]interface{}{"blob": map[string]interface{}{"data": base64.StdEncoding.EncodeToString([]byte("workbook"))}}
	result, protocolErr := NewHandler(plan, invocation.New(invocation.Config{Invoker: component})).Handle(context.WithValue(context.Background(), binaryContextKey{}, "caller"), request)
	require.Nil(t, protocolErr)
	require.NotNil(t, result)
}

func TestBinaryOriginalRawPrecedenceStillValidatesMultipart(t *testing.T) {
	type input struct {
		Files []*multipart.FileHeader `mcp:"name=Files"`
		Body  *originalHybridBody     `mcp:"name=Body"`
	}
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/files", Name: "upload"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "files.upload"}, Contract: testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{
		{Path: "Files", Location: bindstate.Location{Kind: "form", In: "file"}},
		{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}},
	})})
	require.NoError(t, err)
	args := map[string]interface{}{"Files": originalBlob("multipart"), "Blob": originalBlob("raw"), "Payload": map[string]interface{}{"ownerId": 9}}
	scope, err := plan.Scope(args)
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Value(context.Background(), reflect.TypeOf(&originalHybridBody{}), "body")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("raw"), value.(*originalHybridBody).Raw)
	args["Payload"] = map[string]interface{}{"file": "override"}
	_, err = plan.Scope(args)
	require.ErrorContains(t, err, "conflicts with a declared form parameter")
}

func TestBinaryRawBlobPreservesDeclaredFormWithoutFiles(t *testing.T) {
	type input struct {
		Body   *originalHybridBody `mcp:"name=Body"`
		Action string              `mcp:"name=Action"`
	}
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/files", Name: "upload"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "files.upload"}, Contract: testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{
		{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}},
		{Path: "Action", Location: bindstate.Location{Kind: "form", In: "uploadAction"}},
	})})
	require.NoError(t, err)
	scope, err := plan.Scope(map[string]interface{}{"Blob": originalBlob("raw"), "Action": "import"})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Form().Locate(nil).Value(context.Background(), reflect.TypeOf(""), "uploadAction")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "import", value)
	value, found, err = scope.Body().Value(context.Background(), reflect.TypeOf(&originalHybridBody{}), "body")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("raw"), value.(*originalHybridBody).Raw)
}
