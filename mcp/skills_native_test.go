package mcp_test

import (
	"context"
	"fmt"
	"github.com/viant/datly/application"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"strings"
	"testing"
	"testing/fstest"
)

func TestNativeSkillsPrivateReload(t *testing.T) {
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			ctx := context.Background()
			manager, err := application.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Shutdown(ctx)
			files := fstest.MapFS{"SKILL.md": {Data: []byte("---\nname: private\ndescription: Private.\n---\nfirst")}}
			rule := &authorization.Authorization{RequiredScopes: []string{"read"}, ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://private.example"}}
			compile := func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
				return &application.Build{MCP: mcp.Config{Folders: []resource.Folder{{Namespace: "docs", FS: files, URIPrefix: "skill://private/", Skills: []string{"."}}}, Authorization: &authorization.Policy{Global: rule}}}, nil
			}
			if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: compile}); err != nil {
				t.Fatal(err)
			}
			native := (mcpclient.Config{Source: manager, ProtocolVersion: version, ResourceAuthorizer: func(_ context.Context, token *authorization.Token, rule *authorization.Authorization) error {
				if token == nil || strings.TrimPrefix(token.Token, "Bearer ") != "operator-verified" || len(rule.RequiredScopes) != 1 || rule.RequiredScopes[0] != "read" {
					return fmt.Errorf("unverified")
				}
				return nil
			}}).New(t)
			for _, token := range []string{"", "forged"} {
				if _, err = native.ListSkills(ctx, nil, client.WithAuthToken(token)); err == nil {
					t.Fatal("private metadata disclosed")
				}
				if _, err = native.ReadResource(ctx, &schema.ReadResourceRequestParams{Uri: "skill://private/SKILL.md"}, client.WithAuthToken(token)); err == nil {
					t.Fatal("private bytes disclosed")
				}
			}
			list, err := native.ListSkills(ctx, nil, client.WithAuthToken("operator-verified"))
			if err != nil || len(list.Skills) != 1 {
				t.Fatalf("authorized skills %+v %v", list, err)
			}
			files["SKILL.md"].Data = []byte("---\nname: private\ndescription: Updated.\n---\nsecond")
			if err = manager.Reload(ctx, application.Request{Revision: 2, Compile: compile}); err != nil {
				t.Fatal(err)
			}
			get, err := native.GetSkill(ctx, list.Skills[0].Uri, client.WithAuthToken("operator-verified"))
			if err != nil {
				t.Fatal(err)
			}
			if get.Skill.Resources.Files[0].Digest == list.Skills[0].Resources.Files[0].Digest {
				t.Fatal("stale metadata after reload")
			}
			read, err := native.ReadResource(ctx, &schema.ReadResourceRequestParams{Uri: get.Skill.Uri}, client.WithAuthToken("operator-verified"))
			if err != nil || !strings.Contains(read.Contents[0].Text, "second") {
				t.Fatalf("updated content %+v %v", read, err)
			}
		})
	}
}
