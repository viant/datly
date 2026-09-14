package resource

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

func TestDeclaredSkillsNestedInventoryAndIsolation(t *testing.T) {
	files := fstest.MapFS{
		"published/SKILL.md":              {Data: []byte("---\nname: demo\ndescription: Parent.\nlicense: MIT\nmetadata: {version: '1'}\nfuture: {enabled: true}\n---\nParent")},
		"published/nested/demo/SKILL.md":  {Data: []byte("---\nname: demo\ndescription: Nested.\n---\nChild")},
		"published/nested/demo/empty.txt": {},
		"published/raw.txt":               {Data: []byte{0xff, 0xfe}},
		"private.txt":                     {Data: []byte("unpublished")},
	}
	store := bindresource.New()
	if err := store.Register("docs", files); err != nil {
		t.Fatal(err)
	}
	publisher := Publisher{Resources: store}
	folder := Folder{Namespace: "docs", Root: "published", URIPrefix: "docs://team/demo/", Skills: []string{".", "nested/demo"}}
	plans, err := publisher.Compile(context.Background(), []Folder{folder})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(plans)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(catalog, nil)
	registry := protocol.NewRegistry()
	for _, file := range catalog.Resources() {
		registry.RegisterResource(file, handler.Handle)
	}
	if err = catalog.RegisterSkills(registry); err != nil {
		t.Fatal(err)
	}
	entries := registry.ListRegisteredSkills()
	if len(entries) != 2 || len(entries[0].Resources.Files) != 4 || len(entries[1].Resources.Files) != 2 {
		t.Fatalf("nested entries %+v", entries)
	}
	files["published/SKILL.md"].Data = []byte("changed after snapshot")
	for _, entry := range entries {
		for _, file := range entry.Resources.Files {
			read, e := handler.Handle(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: file.Uri}})
			if e != nil {
				t.Fatal(e)
			}
			data := []byte(read.Contents[0].Text)
			if read.Contents[0].Blob != "" {
				data, err = base64.StdEncoding.DecodeString(read.Contents[0].Blob)
				if err != nil {
					t.Fatal(err)
				}
			}
			if file.Size != int64(len(data)) || file.Digest != fmt.Sprintf("sha256:%x", sha256.Sum256(data)) {
				t.Fatalf("raw content mismatch %s", file.Uri)
			}
		}
	}
	for _, uri := range []string{"docs://team/demo/../private.txt", "docs://team/demo/%2e%2e/private.txt", "docs://team/private.txt"} {
		if _, e := handler.Handle(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: uri}}); e == nil {
			t.Fatalf("unpublished URI %s", uri)
		}
	}
	entries[0].Frontmatter["future"].(map[string]interface{})["enabled"] = false
	if reflect.DeepEqual(entries, registry.ListRegisteredSkills()) {
		t.Fatal("skill metadata not detached")
	}
}

func TestDeclaredSkillsValidationNotSchemeClassification(t *testing.T) {
	files := fstest.MapFS{"SKILL.md": {Data: []byte("---\nnot-a-skill: true\n---\nAn ordinary document.")}}
	ordinary := Folder{Namespace: "docs", FS: files, URIPrefix: "skill://ordinary/"}
	plans, err := (Publisher{}).Compile(context.Background(), []Folder{ordinary})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(plans)
	if err != nil {
		t.Fatal(err)
	}
	registry := protocol.NewRegistry()
	if err = catalog.RegisterSkills(registry); err != nil {
		t.Fatal(err)
	}
	if registry.ImplementsSkills() {
		t.Fatal("scheme or filename classified as skill")
	}
	ordinary.Skills = []string{"."}
	if _, err = (Publisher{}).Compile(context.Background(), []Folder{ordinary}); err == nil {
		t.Fatal("invalid declared skill accepted")
	}
	files["SKILL.md"].Data = []byte("---\nname: other\ndescription: Wrong root.\n---\n")
	if _, err = (Publisher{}).Compile(context.Background(), []Folder{ordinary}); err == nil || !strings.Contains(err.Error(), "root") {
		t.Fatalf("URI/name mismatch %v", err)
	}
	ordinary.Skills = []string{"../outside"}
	if _, err = (Publisher{}).Compile(context.Background(), []Folder{ordinary}); err == nil {
		t.Fatal("outside root accepted")
	}
}
