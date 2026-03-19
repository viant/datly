package xgen

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	shapeload "github.com/viant/datly/repository/shape/load"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/types"
)

func TestComponentCodegen_ViewInput_UsesResolvedViewType(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "update")

	component := &shapeload.Component{
		Method:   "POST",
		URI:      "/v1/api/shape/dev/auth/products/",
		RootView: "ProductUpdate",
		Input: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "Jwt", In: state.NewHeaderLocation("Authorization"), Schema: &state.Schema{DataType: "string"}}},
			{Parameter: state.Parameter{Name: "Ids", In: state.NewBodyLocation("Ids"), Schema: &state.Schema{DataType: "[]int"}}},
			{Parameter: state.Parameter{Name: "Records", In: state.NewViewLocation("Records"), Schema: nil}},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "ProductUpdate",
			Mode: view.ModeExec,
		},
		&view.View{
			Name: "Records",
			Schema: &state.Schema{
				Name:        "RecordsView",
				DataType:    "*RecordsView",
				Cardinality: state.Many,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "STATUS", DataType: "int", Nullable: true},
				{Name: "IS_AUTH", DataType: "int", Nullable: true},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "update",
		PackagePath: "github.com/acme/project/shape/dev/vendor/update",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if strings.Contains(generated, "Records []interface {}") {
		t.Fatalf("expected typed Records input field, got interface slice:\n%s", generated)
	}
	if !strings.Contains(generated, "Records []") || !strings.Contains(generated, "RecordsView") {
		t.Fatalf("expected Records view input field to reference RecordsView:\n%s", generated)
	}
	if !strings.Contains(generated, `Status *int `+"`"+`sqlx:"STATUS" velty:"names=STATUS|Status"`+"`") {
		t.Fatalf("expected exec view input helper type to retain velty aliases:\n%s", generated)
	}
	if !strings.Contains(generated, `IsAuth *int `+"`"+`sqlx:"IS_AUTH" velty:"names=IS_AUTH|IsAuth"`+"`") {
		t.Fatalf("expected exec view input helper type to retain SQL alias velty names:\n%s", generated)
	}
}

func TestComponentCodegen_InputSynthesizesRoutePathParams(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "team", "delete")

	component := &shapeload.Component{
		Method:   "DELETE",
		URI:      "/v1/api/shape/dev/team/{teamID}",
		RootView: "Team",
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Team",
		Mode: view.ModeExec,
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "delete",
		PackagePath: "github.com/acme/project/shape/dev/team/delete",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if !strings.Contains(generated, "TeamID string") {
		t.Fatalf("expected implicit route path parameter in generated input:\n%s", generated)
	}
	if !strings.Contains(generated, `parameter:"teamID,kind=path,in=teamID"`) {
		t.Fatalf("expected TeamID path parameter tag in generated input:\n%s", generated)
	}
	if !strings.Contains(generated, `velty:"names=TeamID|teamID"`) {
		t.Fatalf("expected TeamID path parameter velty aliases in generated input:\n%s", generated)
	}
}

func TestComponentCodegen_ExportsLowercaseInputFieldNames(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "env")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/vendors-env/",
		RootView: "Vendor",
		Input: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "vendorIDs", In: state.NewQueryLocation("vendorIDs"), Schema: &state.Schema{DataType: "[]int", Cardinality: state.Many}}},
			{Parameter: state.Parameter{Name: "Vendor", In: state.NewConstLocation("Vendor"), Value: "VENDOR", Tag: `internal:"true"`, Schema: &state.Schema{DataType: "string", Cardinality: state.One}}},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Vendor",
		Mode: view.ModeQuery,
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "env",
		PackagePath: "github.com/acme/project/shape/dev/vendor/env",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if !strings.Contains(generated, "VendorIDs ") {
		t.Fatalf("expected exported generated field for lowercase query input:\n%s", generated)
	}
	if !strings.Contains(generated, `parameter:"vendorIDs,kind=query,in=vendorIDs"`) {
		t.Fatalf("expected original query parameter name to be preserved in tag:\n%s", generated)
	}
}

func TestComponentCodegen_ReadView_OmitsVeltyTags(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "list")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/vendors/",
		RootView: "Vendor",
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Vendor",
		Mode: view.ModeQuery,
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
			{Name: "NAME", DataType: "string", Nullable: true},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "list",
		PackagePath: "github.com/acme/project/shape/dev/vendor/list",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if strings.Contains(generated, `velty:"names=ID|Id"`) || strings.Contains(generated, `velty:"names=NAME|Name"`) {
		t.Fatalf("expected read view fields to omit velty tags:\n%s", generated)
	}
}

func TestComponentCodegen_ReadInput_OmitsVeltyTags(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "list")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/vendors/",
		RootView: "Vendor",
		Input: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "VendorName", In: state.NewFormLocation("name"), Schema: &state.Schema{DataType: "string"}}},
			{Parameter: state.Parameter{Name: "Fields", In: state.NewQueryLocation("fields"), Schema: &state.Schema{DataType: "[]string", Cardinality: state.Many}}},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Vendor",
		Mode: view.ModeQuery,
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "list",
		PackagePath: "github.com/acme/project/shape/dev/vendor/list",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if strings.Contains(generated, `velty:"names=`) {
		t.Fatalf("expected read input fields to omit velty tags:\n%s", generated)
	}
}

func TestComponentCodegen_HandlerExec_OmitsVeltyTags(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "auth")

	component := &shapeload.Component{
		Method:   "POST",
		URI:      "/v1/api/shape/dev/auth/vendor",
		RootView: "Auth",
		ComponentRoutes: []*shapeplan.ComponentRoute{{
			Name:      "Auth",
			RoutePath: "/v1/api/shape/dev/auth/vendor",
			Method:    "POST",
			Handler:   "github.com/acme/project/shape/dev/vendor/auth.Handler",
		}},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Auth",
		Mode: view.ModeHandler,
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
			{Name: "NAME", DataType: "string", Nullable: true},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "auth",
		PackagePath: "github.com/acme/project/shape/dev/vendor/auth",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if strings.Contains(generated, `velty:"names=ID|Id"`) || strings.Contains(generated, `velty:"names=NAME|Name"`) {
		t.Fatalf("expected handler-generated fields to omit velty tags:\n%s", generated)
	}
	if !strings.Contains(generated, `handler=github.com/acme/project/shape/dev/vendor/auth.Handler`) {
		t.Fatalf("expected generated router tag to include handler reference:\n%s", generated)
	}
}

func TestComponentCodegen_CodecBackedInput_UsesCodecResultType(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "user_acl")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/auth/user-acl",
		RootView: "UserAcl",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Jwt",
					In:     state.NewHeaderLocation("Authorization"),
					Schema: &state.Schema{DataType: "string", Cardinality: state.One},
					Output: &state.Codec{Name: "JwtClaim"},
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "UserAcl",
		Mode: view.ModeQuery,
		Columns: []*view.Column{
			{Name: "UserID", DataType: "int"},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "user_acl",
		PackagePath: "github.com/acme/project/shape/dev/vendor/user_acl",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if !strings.Contains(generated, `"github.com/viant/scy/auth/jwt"`) {
		t.Fatalf("expected generated input to import jwt claims package:\n%s", generated)
	}
	if strings.Contains(generated, `"github.com/golang-jwt/jwt/v5"`) {
		t.Fatalf("expected generated input to avoid nested jwt dependency import drift:\n%s", generated)
	}
	if !strings.Contains(generated, "Jwt *jwt.Claims") {
		t.Fatalf("expected codec-backed Jwt field to use codec result type:\n%s", generated)
	}
	if !strings.Contains(generated, `dataType=string`) ||
		!strings.Contains(generated, `codec:"JwtClaim"`) {
		t.Fatalf("expected Jwt field tag to preserve raw datatype and codec metadata:\n%s", generated)
	}
}

func TestComponentCodegen_ViewInput_OverridesStaleInlineSchemaType(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "team", "user_team")

	staleFieldType := reflect.StructOf([]reflect.StructField{
		{Name: "Id", Type: reflect.TypeOf(""), Tag: `json:"id,omitempty"`},
		{Name: "TeamMembers", Type: reflect.TypeOf(""), Tag: `json:"teamMembers,omitempty"`},
		{Name: "Name", Type: reflect.TypeOf(""), Tag: `json:"name,omitempty"`},
	})

	component := &shapeload.Component{
		Method:   "PUT",
		URI:      "/v1/api/shape/dev/teams",
		RootView: "UserTeam",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "TeamStats",
					In:     state.NewViewLocation("TeamStats"),
					Schema: state.NewSchema(reflect.SliceOf(staleFieldType)),
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "UserTeam",
			Mode: view.ModeExec,
		},
		&view.View{
			Name: "TeamStats",
			Schema: &state.Schema{
				Name:        "TeamStatsView",
				DataType:    "*TeamStatsView",
				Cardinality: state.Many,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "TEAM_MEMBERS", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "user_team",
		PackagePath: "github.com/acme/project/shape/dev/team/user_team",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if strings.Contains(generated, "TeamStats []struct") {
		t.Fatalf("expected TeamStats view input field to use resolved TeamStatsView, got anonymous struct:\n%s", generated)
	}
	if !strings.Contains(generated, "TeamStats []") || !strings.Contains(generated, "TeamStatsView") {
		t.Fatalf("expected TeamStats view input field to reference TeamStatsView:\n%s", generated)
	}
}

func TestComponentCodegen_ViewInput_ResolvesSnakeCaseViewName(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "team", "user_team")

	staleFieldType := reflect.StructOf([]reflect.StructField{
		{Name: "Id", Type: reflect.TypeOf(""), Tag: `json:"id,omitempty"`},
	})

	component := &shapeload.Component{
		Method:   "PUT",
		URI:      "/v1/api/shape/dev/teams",
		RootView: "UserTeam",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "TeamStats",
					In:     state.NewViewLocation("TeamStats"),
					Schema: state.NewSchema(reflect.SliceOf(staleFieldType)),
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{Name: "UserTeam", Mode: view.ModeExec},
		&view.View{
			Name: "team_stats",
			Schema: &state.Schema{
				Name:        "TeamStatsView",
				DataType:    "*TeamStatsView",
				Cardinality: state.Many,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "user_team",
		PackagePath: "github.com/acme/project/shape/dev/team/user_team",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if strings.Contains(generated, "UserTeamTeamStatsView") {
		t.Fatalf("expected snake_case resource view to resolve to TeamStatsView, got stale nested helper:\n%s", generated)
	}
	if !strings.Contains(generated, "TeamStats []*TeamStatsView") {
		t.Fatalf("expected TeamStats view input field to reference TeamStatsView:\n%s", generated)
	}
}

func TestComponentCodegen_ExecWithoutExplicitOutput_DoesNotSynthesizeReaderData(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "update")

	component := &shapeload.Component{
		Method:   "POST",
		URI:      "/v1/api/shape/dev/auth/products/",
		RootView: "ProductUpdate",
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "ProductUpdate",
		Mode: view.ModeExec,
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "update",
		PackagePath: "github.com/acme/project/shape/dev/vendor/update",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if strings.Contains(generated, "parameter:\",kind=output,in=view\"") {
		t.Fatalf("did not expect default reader output field for exec component:\n%s", generated)
	}
}

func TestComponentCodegen_ImportsNamedNonStructFieldTypes(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "user", "mysql_boolean")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/user-metadata",
		RootView: "UserMetadata",
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "UserMetadata",
		Schema: state.NewSchema(reflect.TypeOf([]struct {
			ID        int
			IsEnabled *types.BitBool
		}{})),
		Columns: []*view.Column{
			{Name: "ID", DataType: "int"},
			{Name: "IS_ENABLED", DataType: "types.BitBool", Nullable: true},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "mysql_boolean",
		PackagePath: "github.com/acme/project/shape/dev/user/mysql_boolean",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if !strings.Contains(generated, `"github.com/viant/sqlx/types"`) {
		t.Fatalf("expected generated source to import github.com/viant/sqlx/types:\n%s", generated)
	}
}

func TestComponentCodegen_EmitsNamedInputHasHelper(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "user", "mysql_boolean")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/user-metadata",
		RootView: "UserMetadata",
		Input: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "Fields", In: state.NewQueryLocation("fields"), Schema: &state.Schema{DataType: "[]string"}}},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "UserMetadata",
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "mysql_boolean",
		PackagePath: "github.com/acme/project/shape/dev/user/mysql_boolean",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if !strings.Contains(generated, "type UserMetadataInputHas struct") {
		t.Fatalf("expected named UserMetadataInputHas helper declaration:\n%s", generated)
	}
}

func TestComponentCodegen_ExecWithoutStatusDoesNotImportResponse(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "team", "delete")

	component := &shapeload.Component{
		Method:   "DELETE",
		URI:      "/v1/api/shape/dev/team/{teamID}",
		RootView: "Team",
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name: "Team",
		Mode: view.ModeExec,
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "delete",
		PackagePath: "github.com/acme/project/shape/dev/team/delete",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if strings.Contains(generated, `"github.com/viant/xdatly/handler/response"`) {
		t.Fatalf("did not expect response import for empty exec output:\n%s", generated)
	}
}

func TestComponentCodegen_MutableView_EmbedsHasMarker(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "events", "patch_basic_one")

	component := &shapeload.Component{
		Method:   "PATCH",
		URI:      "/v1/api/shape/dev/basic/foos",
		RootView: "foos",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name: "Foos",
					In:   state.NewBodyLocation(""),
					Schema: &state.Schema{
						Name:        "FoosView",
						DataType:    "*FoosView",
						Cardinality: state.One,
					},
					Tag: `anonymous:"true" typeName:"FoosView"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name: "CurFoos",
					In:   state.NewViewLocation("CurFoos"),
					Schema: &state.Schema{
						Name:        "FoosView",
						DataType:    "*FoosView",
						Cardinality: state.One,
					},
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "foos",
			Mode: view.ModeExec,
			Template: &view.Template{
				Source: "#set($_ = $Foos<?>(body/).Required())",
			},
			Schema: &state.Schema{Name: "FoosView", Cardinality: state.Many},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
				{Name: "QUANTITY", DataType: "int", Nullable: true},
			},
		},
		&view.View{
			Name:   "CurFoos",
			Mode:   view.ModeQuery,
			Schema: &state.Schema{Name: "FoosView", Cardinality: state.Many},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
				{Name: "QUANTITY", DataType: "int", Nullable: true},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "patch_basic_one",
		PackagePath: "github.com/acme/project/shape/dev/events/patch_basic_one",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	if !strings.Contains(generated, "type FoosViewHas struct") {
		t.Fatalf("expected mutable helper type declaration:\n%s", generated)
	}
	if !strings.Contains(generated, `Has *FoosViewHas `+"`"+`setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-" typeName:"FoosViewHas"`+"`") {
		t.Fatalf("expected mutable view to embed Has marker:\n%s", generated)
	}
	if !strings.Contains(generated, `Foos *FoosView `+"`"+`parameter:",kind=body" typeName:"FoosView" anonymous:"true"`+"`") {
		t.Fatalf("expected mutable body input to stay pointer typed:\n%s", generated)
	}
}
