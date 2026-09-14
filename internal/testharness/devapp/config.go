package devapp

import (
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/mcp/developer"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe"
	"path/filepath"
	"reflect"
)

const Module = "example.com/mcpapp"
const Package = "github.com/viant/datly/internal/testharness/devapp"

var ReadDQL = "#setting($_ = $route('/records/{id}', 'GET'))\n#define($_ = $ID<int>(path/id))\n#define($_ = $Rows(output/view))\nSELECT id,name FROM records WHERE id=:ID"
var WriteDQL = ""

// Configuration is production composition for the linked test executable, not a Build stub.
func Configuration(root, dsn string) (developer.Config, error) {
	exports, err := Exports()
	if err != nil {
		return developer.Config{}, err
	}
	builder, err := bootstrap.NewArtifactBuilder(exports)
	if err != nil {
		return developer.Config{}, err
	}
	types, err := builder.Catalog(nil)
	if err != nil {
		return developer.Config{}, err
	}
	reader, writer := filepath.Join(root, "reader"), filepath.Join(root, "writer")
	result := developer.Config{Targets: map[string]transcribe.Validator{
		"reader": {BaseDir: reader, Include: []string{Module + "/reader/generated"}},
		"writer": {BaseDir: writer, Include: []string{Module + "/writer/generated"}},
		"app":    {BaseDir: root, ModuleDirs: []string{reader, writer}, Include: []string{Module + "/reader/generated", Module + "/writer/generated"}},
	}, Authoring: map[string]transcribe.Request{}, Applications: map[string]developer.Application{}}
	result.Authoring["reader"] = transcribe.Request{Destination: reader, Source: &transcribe.Source{Scope: Package, Name: "Read", Connector: "main", Types: types}, Component: &bootstrap.RouteSource{PackagePath: Package, PackageName: "devapp", FieldName: "Read", InputType: "ReadInput", OutputType: "ReadOutput", Tag: dtag.Component{Method: "GET", Path: "/records/{id}", View: "Read", Connector: "main"}}, InputType: reflect.TypeFor[ReadInput](), OutputType: reflect.TypeFor[ReadOutput]()}
	result.Authoring["writer"] = transcribe.Request{Destination: writer, Source: &transcribe.Source{Scope: Package, Name: "Write", Connector: "main", Types: types}, Component: &bootstrap.RouteSource{PackagePath: Package, PackageName: "devapp", FieldName: "Write", InputType: "WriteInput", OutputType: "WriteOutput", Tag: dtag.Component{Method: "POST", Path: "/records", Handler: Package + ".NewWrite", Connector: "main"}}, InputType: reflect.TypeFor[WriteInput](), OutputType: reflect.TypeFor[WriteOutput]()}
	result.Applications["app"] = developer.Application{Options: standalone.Options{Registry: exports, Config: &config.Config{BaseDir: root, ModuleDirs: []string{reader, writer}, Connector: "main", Connectors: []connector.Config{{Name: "main", Driver: "sqlite3", DSN: dsn}}, GoBootstrap: &config.Packages{Packages: []string{Module + "/reader/generated", Module + "/writer/generated"}}}}}
	return result, nil
}
