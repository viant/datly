package mcp_test

import (
	"context"
	"github.com/viant/datly/application"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"testing"
	"testing/fstest"
)

func TestMappedPolicyReloadAndPinnedResources(t *testing.T) {
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	files := fstest.MapFS{"guide.md": &fstest.MapFile{Data: []byte("first")}}
	policy := &authorization.Policy{Resources: map[string]*authorization.Authorization{"docs://app/guide.md": {RequiredScopes: []string{"docs"}, ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://app.example"}}}}
	compile := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{MCP: mcp.Config{Folders: []resource.Folder{{Namespace: "help", FS: files, URIPrefix: "docs://app/"}}, Authorization: policy}}, nil
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	pinned, first, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	files["guide.md"].Data = []byte("second")
	if err = manager.Reload(context.Background(), application.Request{Revision: 2, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ctx  context.Context
		want string
	}{{pinned, "first"}, {context.Background(), "second"}} {
		_, service, err := manager.Pin(tc.ctx)
		if err != nil {
			t.Fatal(err)
		}
		read, e := service.ReadResource(tc.ctx, &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "docs://app/guide.md"}})
		if e != nil || read.Contents[0].Text != tc.want {
			t.Fatalf("read %+v %v", read, e)
		}
		if service.Authorization().Resources["docs://app/guide.md"].RequiredScopes[0] != "docs" {
			t.Fatal("policy lost")
		}
	}
	first.Authorization().Resources["docs://app/guide.md"].RequiredScopes[0] = "mutated"
	if first.Authorization().Resources["docs://app/guide.md"].RequiredScopes[0] != "docs" {
		t.Fatal("policy mutation escaped")
	}
	policy.Resources["docs://app/missing"] = &authorization.Authorization{ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://app.example"}}
	if err = manager.Reload(context.Background(), application.Request{Revision: 3, Compile: compile}); err == nil || manager.Revision() != 2 {
		t.Fatal("unknown resource policy published")
	}
}

func TestNativeMappedResourceAuthorization(t *testing.T) {
	rt, err := runtime.NewRuntime(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	service, err := mcp.New(mcp.Config{Invoker: rt, Folders: []resource.Folder{{Namespace: "protected", FS: fstest.MapFS{"readme.md": &fstest.MapFile{Data: []byte("protected-content")}}, URIPrefix: "docs://protected/"}}, Authorization: &authorization.Policy{Global: &authorization.Authorization{RequiredScopes: []string{"docs"}, ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://protected.example"}}}})
	if err != nil {
		t.Fatal(err)
	}
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	if _, err = native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: "docs://protected/readme.md"}); err == nil {
		t.Fatal("protected mapped content was disclosed without authorization")
	}
}
