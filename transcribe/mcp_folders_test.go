package transcribe_test

import (
	"context"
	"fmt"
	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/mcp-protocol/schema"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestDQLFoldersGeneratedEmbeddedGatewayBinary(t *testing.T) {
	for _, documented := range []bool{false, true} {
		t.Run(fmt.Sprintf("documentation_%v", documented), func(t *testing.T) {
			testDQLFoldersGeneratedEmbeddedGatewayBinary(t, documented)
		})
	}
}

func testDQLFoldersGeneratedEmbeddedGatewayBinary(t *testing.T, documented bool) {
	root, outside := t.TempDir(), t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/folderapp"}).Write(t, root)
	dsn := filepath.Join(outside, "db.sqlite")
	db := sqlite.New(t, sqlite.WithDSN(dsn))
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER NOT NULL,name TEXT NOT NULL)", "INSERT INTO records VALUES(1,'embedded')"); err != nil {
		t.Fatal(err)
	}
	store := bindresource.New()
	if err := store.Register("docs", fstest.MapFS{"guide/SKILL.md": &fstest.MapFile{Data: []byte("---\nname: app-guide\ndescription: Use the application tools.\n---\nRead [guide](references/guide.md).")}, "guide/references/guide.md": &fstest.MapFile{Data: []byte("embedded-reference")}, "unpublished.txt": &fstest.MapFile{Data: []byte("private")}, "annotation.yaml": &fstest.MapFile{Data: []byte("Paths:\n  /records: Jointly embedded records\n")}}); err != nil {
		t.Fatal(err)
	}
	source := `#setting($_ = $route('/records', 'GET'))
#setting($_ = $mcp('records.read', 'Read records'))
#setting($_ = $mcp_skill_folder('docs', 'guide', 'skill://app-guide/'))
SELECT id,name FROM records`
	if documented {
		source = strings.Replace(source, "SELECT id,name FROM records", "#setting($_ = $DocURLs('docs:annotation.yaml'))\nSELECT id,name FROM records", 1)
		source = strings.Replace(source, "$mcp('records.read', 'Read records')", "$mcp('records.read')", 1)
	}
	generated, err := transcribe.NewCompiler().Transcribe(context.Background(), transcribe.Request{Destination: root, Source: &transcribe.Source{Scope: "example.com/folderapp", Name: "Records", Text: source, Resources: store, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Result.Plan.Settings.MCPFolders) != 1 {
		t.Fatal("folder declaration lost")
	}
	program := fmt.Sprintf(gatewayProgram, generated.Result.Plan.Input.Type, generated.Result.Plan.Output.Type)
	if err = os.WriteFile(filepath.Join(root, "main.go"), []byte(program), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(outside, "gateway")
	build := exec.Command("go", "build", "-mod=mod", "-o", binary, ".")
	build.Dir = root
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, output)
	}
	if err = os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	t.Chdir(outside)
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			native := (mcpclient.StdioConfig{Binary: binary, Args: []string{dsn}, ProtocolVersion: version}).New(t)
			init, err := native.Initialize(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := init.Capabilities.Extensions[schema.SkillsExtension]; !ok {
				t.Fatal("generated skills extension absent")
			}
			entries, err := native.ListSkills(context.Background(), nil)
			if err != nil || len(entries.Skills) != 1 || len(entries.Skills[0].Resources.Files) != 2 {
				t.Fatalf("skills inventory: %+v %v", entries, err)
			}
			entry, err := native.GetSkill(context.Background(), entries.Skills[0].Uri)
			if err != nil {
				t.Fatal(err)
			}
			mcpclient.SkillFile(t, native, entry.Skill, entry.Skill.Uri)
			mcpclient.SkillFile(t, native, entry.Skill, "skill://app-guide/references/guide.md")
			listing, err := native.ListResources(context.Background(), nil)
			if err != nil || len(listing.Resources) != 2 {
				t.Fatalf("resources %+v %v", listing, err)
			}
			read, err := native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: "skill://app-guide/references/guide.md"})
			if err != nil || read.Contents[0].Text != "embedded-reference" {
				t.Fatalf("embedded reference %+v %v", read, err)
			}
			if _, err = native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: "skill://app-guide/../unpublished.txt"}); err == nil {
				t.Fatal("unpublished file read")
			}
			tools, err := native.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatalf("tools inventory: %+v %v", tools, err)
			}
			byName := map[string]schema.Tool{}
			for _, tool := range tools.Tools {
				byName[tool.Name] = tool
			}
			if len(tools.Tools) != 3 || len(byName) != 3 || byName["records.read"].Name == "" || byName["skills/get"].Name == "" || byName["skills/list"].Name == "" {
				t.Fatalf("business and standard skills tools: %+v", tools.Tools)
			}
			business := byName["records.read"]
			if documented && (business.Description == nil || *business.Description != "Jointly embedded records") {
				t.Fatalf("embedded documentation missing from tool: %+v", business)
			}
			result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.read", Arguments: map[string]any{}})
			if err != nil || result.IsError != nil && *result.IsError {
				t.Fatalf("business tool %+v %v", result, err)
			}
			if !strings.Contains(fmt.Sprint(result), "embedded") {
				t.Fatalf("typed SQLite result %+v", result)
			}
		})
	}
}

const gatewayProgram = `package main
import("context";"database/sql";"fmt";"os";"reflect";_ "github.com/mattn/go-sqlite3";g "example.com/folderapp/generated";"github.com/viant/datly/bootstrap";"github.com/viant/datly/mcp";"github.com/viant/datly/mcp/server";"github.com/viant/datly/runtime";"github.com/viant/datly/runtime/registry";dsql "github.com/viant/datly/sql";"github.com/viant/datly/tag";"github.com/viant/bindly/resource")
func main(){ctx:=context.Background();db,err:=sql.Open("sqlite3",os.Args[1]);check(err);defer db.Close();store:=resource.New();check(store.Register(g.DatlyResourceNamespace,g.DatlyResources));field:=reflect.TypeOf(g.Component{}).Field(0);metadata,_,err:=tag.ParseComponent(field.Tag);check(err);input,output:=reflect.TypeOf(g.%s{}),reflect.TypeOf(g.%s{});component,err:=(&bootstrap.RouteSource{PackagePath:"example.com/folderapp/generated",PackageName:"generated",FieldName:field.Name,Tag:metadata}).Resolve(input,output);check(err);artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:input,OutputType:output,Resources:store});check(err);reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db}});check(err);entry,err:=artifact.Registration(registry.RegisteredComponent{Reader:reader});check(err);rt,err:=runtime.NewRuntime([]*registry.RegisteredComponent{entry},runtime.WithResources(store));check(err);defer rt.Shutdown(ctx);service,err:=mcp.New(mcp.Config{Components:[]*registry.RegisteredComponent{entry},Invoker:rt,Resources:store});check(err);transport,err:=server.New(server.Config{Service:service,Transport:server.TransportConfig{Kind:server.TransportStdio}});check(err);check(transport.Serve(ctx))}
func check(err error){if err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)}}`
