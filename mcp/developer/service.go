// Package developer exposes explicitly configured authoring tools separately
// from Datly's business component catalog and runtime invocation engine.
package developer

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/viant/datly/mcp/developer/skills"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/transcribe"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
)

const ValidationTool = "datly.validate"
const TranscribeTool = "datly.transcribe"
const RunTool = "datly.run"
const StopTool = "datly.stop"
const ComponentsTool = "datly.components"
const InspectTool = "datly.inspect"
const ReverseTool = "datly.reverseDQL"

// Config is server/operator configuration, never tool-call input. Each target
// fixes its project, module/package selection and optional schema authority.
// Refiners, their connections and authorization policy remain caller-owned and
// must be safe for concurrent use. Validation never closes those connections.
type Config struct {
	Targets       map[string]transcribe.Validator
	Authorization *authorization.Policy
	Authoring     map[string]transcribe.Request
	Applications  map[string]Application
	MaxInstances  int
}

// Application is a trusted linked standalone target. The caller owns immutable
// exports/configuration; clients select only the target and allowed bind options.
type Application struct {
	Options   standalone.Options
	Ports     []int
	Addresses []string
}

// Service implements mcp/server.ServerService with a developer-only registry.
// Construct it explicitly and pass it to the existing MCP server transport.
type Service struct {
	targets      map[string]transcribe.Validator
	registry     *protocolserver.Registry
	policy       *authorization.Policy
	resources    *resource.Handler
	authoring    map[string]transcribe.Request
	applications map[string]Application
	mu           sync.Mutex
	workspace    chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	closed       bool
	work         sync.WaitGroup
	instances    map[string]*instance
	history      map[string]Instance
	order        []string
	limit        int
	done         chan struct{}
	bundled      []*resource.Plan
	documents    map[string]*resource.Plan
}

func New(config Config) (*Service, error) {
	if len(config.Targets) == 0 {
		return nil, fmt.Errorf("developer validation requires configured targets")
	}
	result := &Service{targets: make(map[string]transcribe.Validator, len(config.Targets)), registry: protocolserver.NewRegistry(), policy: config.Authorization}
	names := make([]string, 0, len(config.Targets))
	for name, validator := range config.Targets {
		if name == "" || strings.TrimSpace(name) != name {
			return nil, fmt.Errorf("developer validation target names must be nonempty without surrounding whitespace")
		}
		if strings.TrimSpace(validator.BaseDir) == "" {
			return nil, fmt.Errorf("developer validation target %q requires an explicit base directory", name)
		}
		base, err := filepath.Abs(validator.BaseDir)
		if err != nil {
			return nil, fmt.Errorf("developer validation target %q: %w", name, err)
		}
		validator.BaseDir = base
		validator.ModuleDirs = append([]string(nil), validator.ModuleDirs...)
		validator.Include = append([]string(nil), validator.Include...)
		validator.Exclude = append([]string(nil), validator.Exclude...)
		result.targets[name] = validator
		names = append(names, name)
	}
	sort.Strings(names)
	metadata, err := result.validationMetadata(names)
	if err != nil {
		return nil, err
	}
	result.registry.RegisterTool(&protocolserver.ToolEntry{Metadata: metadata, Handler: result.validate})
	if err := result.configure(config); err != nil {
		return nil, err
	}
	plans, err := (resource.Publisher{}).Compile(context.Background(), skills.Folders())
	if err != nil {
		return nil, err
	}
	catalog, err := resource.NewCatalog(plans)
	if err != nil {
		return nil, err
	}
	result.resources = resource.NewHandler(catalog, nil)
	result.bundled = plans
	for _, file := range catalog.Resources() {
		result.registry.RegisterResource(file, result.resources.Handle)
	}
	if err := catalog.RegisterSkills(result.registry); err != nil {
		return nil, err
	}
	for _, name := range []string{TranscribeTool, RunTool, StopTool, ComponentsTool, InspectTool, ReverseTool} {
		metadata, err := result.toolMetadata(name, names)
		if err != nil {
			return nil, err
		}
		result.registry.RegisterTool(&protocolserver.ToolEntry{Metadata: metadata, Handler: result.execute})
	}
	return result, nil
}

func (s *Service) Registry() *protocolserver.Registry {
	if s == nil {
		return nil
	}
	return s.registry
}

func (s *Service) Authorization() *authorization.Policy {
	if s == nil {
		return nil
	}
	return s.policy
}

// ReadResource publishes only the reproducible embedded authoring bundle.
func (s *Service) ReadResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	s.mu.Lock()
	handler := s.resources
	s.mu.Unlock()
	return handler.Handle(ctx, request)
}

func (s *Service) validationMetadata(names []string) (schema.Tool, error) {
	description := "Validate an operator-configured Datly project/package selection using the same transcribe.Validator as datly validate. Returns its exact report and diagnostics. Read completed and skipped stages: success is not runtime readiness. Static by default; schema discovery runs only for targets with an operator-configured refiner and connector authority. No generated-file writes, component activation, application hooks, DML or fixture execution."
	readOnly := true
	output := &schema.ToolOutputSchema{}
	if err := output.Load(&transcribe.ValidationReport{}); err != nil {
		return schema.Tool{}, fmt.Errorf("developer validation report schema: %w", err)
	}
	return schema.Tool{
		Name: ValidationTool, Description: &description,
		Annotations: &schema.ToolAnnotations{ReadOnlyHint: &readOnly},
		InputSchema: schema.ToolInputSchema{
			Type: "object", Required: []string{"target"},
			Properties: schema.ToolInputSchemaProperties{"target": {"type": "string", "enum": names, "description": "Operator-configured validation target; project paths, package selection and schema authority are fixed by the server. Other arguments are rejected."}},
		},
		OutputSchema: output,
	}, nil
}
