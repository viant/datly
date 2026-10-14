package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/transcribe/internal/handlergen"
)

type ContractOptions = handlergen.ContractOptions
type HandlerTarget = handlergen.HandlerTarget
type WriteOperation = handlergen.WriteOperation
type CurrentBinding = handlergen.CurrentBinding
type Options = handlergen.Options
type HandlerOptions = handlergen.HandlerOptions
type HookOptions = handlergen.HookOptions
type GoHandlerOptions = handlergen.GoHandlerOptions
type GoExecution = handlergen.GoExecution
type VeltyHandlerOptions = handlergen.VeltyHandlerOptions

const (
	ContractsAuto       = handlergen.ContractsAuto
	ContractsGenerated  = handlergen.ContractsGenerated
	ContractsLinked     = handlergen.ContractsLinked
	HandlerNone         = handlergen.HandlerNone
	HandlerVelty        = handlergen.HandlerVelty
	HandlerGo           = handlergen.HandlerGo
	WritePost           = handlergen.WritePost
	WritePut            = handlergen.WritePut
	WritePatch          = handlergen.WritePatch
	GoExecutionDirect   = handlergen.GoExecutionDirect
	GoExecutionMutation = handlergen.GoExecutionMutation
)

// Request is the sole public authored-source-to-package transcription request.
type Request struct {
	Component   *bootstrap.RouteSource
	InputType   reflect.Type
	OutputType  reflect.Type
	Source      *Source
	Destination string
	Options     Options
	Generation  GenerationOptions
}

// GenerationOptions opts a configured authoring target into the high-level
// Generator path. Source and Destination remain owned by Request so operator
// configuration has one source/destination owner.
type GenerationOptions struct {
	Operation string
	Language  HandlerTarget
}

func (o GenerationOptions) Enabled() bool {
	return strings.TrimSpace(o.Operation) != "" || strings.TrimSpace(string(o.Language)) != ""
}

func (o GenerationOptions) Normalize() (GenerationOptions, error) {
	o.Operation = strings.ToLower(strings.TrimSpace(o.Operation))
	o.Language = HandlerTarget(strings.ToLower(strings.TrimSpace(string(o.Language))))
	if o.Language == "" {
		o.Language = HandlerGo
	}
	switch o.Operation {
	case "get", "patch", "post", "put":
	default:
		return GenerationOptions{}, fmt.Errorf("gen operation must be get, patch, post or put")
	}
	switch o.Language {
	case HandlerGo, HandlerVelty:
	default:
		return GenerationOptions{}, fmt.Errorf("unsupported gen language %q", o.Language)
	}
	return o, nil
}

func (r Request) NormalizeGeneration() (Request, error) {
	if !r.Generation.Enabled() {
		return r, nil
	}
	generation, err := r.Generation.Normalize()
	if err != nil {
		return Request{}, err
	}
	if r.Component != nil || r.InputType != nil || r.OutputType != nil {
		return Request{}, fmt.Errorf("high-level generation cannot be combined with linked component contracts")
	}
	if handlergen.Explicit(r.Options) {
		return Request{}, fmt.Errorf("high-level generation cannot be combined with low-level transcription options")
	}
	r.Generation = generation
	return r, nil
}

func (r Request) GeneratorRequest() (Generator, GenerationRequest, error) {
	r, err := r.NormalizeGeneration()
	if err != nil {
		return Generator{}, GenerationRequest{}, err
	}
	if !r.Generation.Enabled() {
		return Generator{}, GenerationRequest{}, fmt.Errorf("high-level generation is not enabled")
	}
	return Generator{Operation: r.Generation.Operation, Language: r.Generation.Language}, GenerationRequest{Source: r.Source, Destination: r.Destination}, nil
}
