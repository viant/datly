package transcribe

import (
	"fmt"
	"github.com/viant/datly/bootstrap"
	"reflect"
	"strings"

	handlerast "github.com/viant/datly/transcribe/handler/ast"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
)

// ContractOptions controls contract ownership for one transcription.
type ContractOptions string

const (
	ContractsAuto      ContractOptions = "auto"
	ContractsGenerated ContractOptions = "generated"
	ContractsLinked    ContractOptions = "linked"
)

// HandlerTarget selects a generated handler target. The zero value preserves
// package, supplied, or authored handler authority.
type HandlerTarget string

const (
	HandlerNone  HandlerTarget = "none"
	HandlerVelty HandlerTarget = "velty"
	HandlerGo    HandlerTarget = "go"
)

type WriteOperation = handlerast.Operation
type CurrentBinding = handlercompiler.CurrentBinding

const (
	WritePost  = handlerast.OperationPost
	WritePut   = handlerast.OperationPut
	WritePatch = handlerast.OperationPatch
)

// Options contains target-independent transcription choices.
type Options struct {
	Contracts ContractOptions
	Handler   HandlerOptions
}

// Request is the sole public authored-source-to-package transcription request.
type Request struct {
	Component   *bootstrap.RouteSource
	InputType   reflect.Type
	OutputType  reflect.Type
	Source      *Source
	Destination string
	Options     Options
}

// HandlerOptions selects one generated product over the canonical handler
// plan. It never selects behavior from an HTTP method.
type HandlerOptions struct {
	Target    HandlerTarget
	Operation WriteOperation
	Input     string
	Output    string
	RootView  string
	Current   string
	Currents  []CurrentBinding
	Table     string
	Key       string
	Hooks     HookOptions
	Go        GoHandlerOptions
	Velty     VeltyHandlerOptions
}

// HookOptions opts into a create-once user-owned file. Direct Go execution
// scaffolds input/output lifecycle methods; mutation execution scaffolds typed
// EntityHooks for roles without an authored hook declaration.
type HookOptions struct {
	Scaffold    bool
	Destination string
}

type GoHandlerOptions struct {
	Execution   GoExecution
	Factory     string
	Handler     string
	Destination string
}

// GoExecution selects generated orchestration without changing the Go target
// or the canonical invocation engine. Direct preserves authored control flow.
type GoExecution string

const (
	GoExecutionDirect   GoExecution = "direct"
	GoExecutionMutation GoExecution = "mutation"
)

type VeltyHandlerOptions struct {
	Factory             string
	GoDestination       string
	ResourceDestination string
}

func normalizeOptions(options Options) (Options, error) {
	options.Contracts = ContractOptions(strings.ToLower(strings.TrimSpace(string(options.Contracts))))
	if options.Contracts == "" {
		options.Contracts = ContractsAuto
	}
	switch options.Contracts {
	case ContractsAuto, ContractsGenerated, ContractsLinked:
	default:
		return Options{}, fmt.Errorf("unsupported contract option %q", options.Contracts)
	}
	handlerOptions, err := normalizeHandlerOptions(options.Handler)
	if err != nil {
		return Options{}, err
	}
	options.Handler = handlerOptions
	return options, nil
}

func normalizeHandlerOptions(options HandlerOptions) (HandlerOptions, error) {
	options.Currents = append([]CurrentBinding(nil), options.Currents...)
	options.Target = HandlerTarget(strings.ToLower(strings.TrimSpace(string(options.Target))))
	if options.Target == "" {
		options.Target = HandlerNone
	}
	options.Operation = WriteOperation(strings.ToLower(strings.TrimSpace(string(options.Operation))))
	options.Input = strings.TrimSpace(options.Input)
	options.Output = strings.TrimSpace(options.Output)
	options.RootView = strings.TrimSpace(options.RootView)
	options.Current = strings.TrimSpace(options.Current)
	options.Table = strings.TrimSpace(options.Table)
	options.Key = strings.TrimSpace(options.Key)
	options.Hooks.Destination = strings.TrimSpace(options.Hooks.Destination)
	options.Go.Factory = strings.TrimSpace(options.Go.Factory)
	options.Go.Execution = GoExecution(strings.ToLower(strings.TrimSpace(string(options.Go.Execution))))
	options.Go.Handler = strings.TrimSpace(options.Go.Handler)
	options.Go.Destination = strings.TrimSpace(options.Go.Destination)
	options.Velty.Factory = strings.TrimSpace(options.Velty.Factory)
	options.Velty.GoDestination = strings.TrimSpace(options.Velty.GoDestination)
	options.Velty.ResourceDestination = strings.TrimSpace(options.Velty.ResourceDestination)
	for index := range options.Currents {
		options.Currents[index].ViewIdentity = strings.TrimSpace(options.Currents[index].ViewIdentity)
		options.Currents[index].Param = strings.TrimSpace(options.Currents[index].Param)
	}
	switch options.Target {
	case HandlerNone:
		if hasGeneratedHandlerOptions(options) {
			return HandlerOptions{}, fmt.Errorf("handler target is required when generated handler options are set")
		}
	case HandlerVelty, HandlerGo:
		if options.Operation == "" {
			return HandlerOptions{}, fmt.Errorf("generated handler operation is required")
		}
	default:
		return HandlerOptions{}, fmt.Errorf("unsupported handler target %q", options.Target)
	}
	if options.Target != HandlerGo && (options.Hooks.Scaffold || options.Hooks.Destination != "" || options.Go != (GoHandlerOptions{})) {
		return HandlerOptions{}, fmt.Errorf("Go handler and hook options require target %q", HandlerGo)
	}
	if options.Target != HandlerVelty && options.Velty != (VeltyHandlerOptions{}) {
		return HandlerOptions{}, fmt.Errorf("Velty handler options require target %q", HandlerVelty)
	}
	if options.Target == HandlerGo {
		if options.Go.Execution == "" {
			options.Go.Execution = GoExecutionDirect
		}
		if options.Hooks.Destination != "" && !options.Hooks.Scaffold {
			return HandlerOptions{}, fmt.Errorf("hook destination requires hook scaffolding")
		}
		switch options.Go.Execution {
		case GoExecutionDirect:
		case GoExecutionMutation:
			if options.Go.Handler != "" {
				return HandlerOptions{}, fmt.Errorf("mutation execution does not use a direct Go handler type")
			}
		default:
			return HandlerOptions{}, fmt.Errorf("unsupported Go execution %q", options.Go.Execution)
		}
	}
	return options, nil
}

func hasGeneratedHandlerOptions(options HandlerOptions) bool {
	return options.Operation != "" || options.Input != "" || options.Output != "" || options.RootView != "" ||
		options.Current != "" || len(options.Currents) != 0 || options.Table != "" || options.Key != "" ||
		options.Hooks.Scaffold || options.Hooks.Destination != "" || options.Go != (GoHandlerOptions{}) ||
		options.Velty != (VeltyHandlerOptions{})
}
