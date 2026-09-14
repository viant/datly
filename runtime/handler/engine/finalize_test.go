package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	xmcp "github.com/viant/xdatly/handler/mcp"
)

type finalizingOutput struct {
	called bool
	err    error
}

func (o *finalizingOutput) Finalize(context.Context) error {
	o.called = true
	return o.err
}

type errorFinalizingOutput struct {
	called   bool
	observed error
	err      error
}

func (o *errorFinalizingOutput) Finalize(_ context.Context, err error) error {
	o.called = true
	o.observed = err
	return o.err
}

type mcpFinalizingOutput struct {
	order      []string
	mcpContext xmcp.Context
	mcpErr     error
}

func (o *mcpFinalizingOutput) Finalize(context.Context) error {
	o.order = append(o.order, "finalize")
	return nil
}

func (o *mcpFinalizingOutput) FinalizeMCP(_ context.Context, value xmcp.Context) error {
	o.order = append(o.order, "mcp")
	o.mcpContext = value
	return o.mcpErr
}

func TestFinalizeRunsSuccessFinalizerOnlyOnSuccess(t *testing.T) {
	output := &finalizingOutput{}
	actual, err := finalizeBeforeCompletion(context.Background(), output, nil)
	if err == nil {
		actual, err = finalizeAfterCompletion(context.Background(), actual)
	}
	if err != nil || actual != output || !output.called {
		t.Fatalf("unexpected successful finalization: actual=%#v called=%v err=%v", actual, output.called, err)
	}

	handlerErr := errors.New("handler failed")
	output = &finalizingOutput{}
	_, err = finalizeBeforeCompletion(context.Background(), output, handlerErr)
	if !errors.Is(err, handlerErr) || output.called {
		t.Fatalf("success finalizer must not run after handler error: called=%v err=%v", output.called, err)
	}
}

func TestFinalizeErrorAwareOutputObservesAndJoinsErrors(t *testing.T) {
	handlerErr := errors.New("handler failed")
	finalizeErr := errors.New("finalize failed")
	output := &errorFinalizingOutput{err: finalizeErr}

	actual, err := finalizeBeforeCompletion(context.Background(), output, handlerErr)
	if actual != output || !output.called || output.observed != handlerErr {
		t.Fatalf("unexpected error-aware finalization: actual=%#v output=%#v", actual, output)
	}
	if !errors.Is(err, handlerErr) || !errors.Is(err, finalizeErr) {
		t.Fatalf("expected joined handler/finalizer errors, got %v", err)
	}
}

func TestFinalizeRunsMCPHookAfterSuccessfulOrdinaryFinalizer(t *testing.T) {
	mcpContext := engineMCPContext{}
	output := &mcpFinalizingOutput{}
	ctx := xmcp.WithContext(context.Background(), mcpContext)
	actual, err := finalizeBeforeCompletion(ctx, output, nil)
	if err == nil {
		actual, err = finalizeAfterCompletion(ctx, actual)
	}
	if err != nil || actual != output {
		t.Fatalf("unexpected MCP finalization: actual=%#v err=%v", actual, err)
	}
	if !reflect.DeepEqual(output.order, []string{"finalize", "mcp"}) || output.mcpContext != mcpContext {
		t.Fatalf("unexpected MCP finalizer state: %#v", output)
	}
}

func TestFinalizeSkipsMCPHookOnHandlerError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	output := &mcpFinalizingOutput{}
	_, err := finalizeBeforeCompletion(xmcp.WithContext(context.Background(), engineMCPContext{}), output, handlerErr)
	if !errors.Is(err, handlerErr) || len(output.order) != 0 {
		t.Fatalf("MCP finalizer must not run on handler failure: order=%v err=%v", output.order, err)
	}
}

func TestFinalizePropagatesMCPFinalizerError(t *testing.T) {
	wantErr := errors.New("MCP finalize failed")
	output := &mcpFinalizingOutput{mcpErr: wantErr}
	ctx := xmcp.WithContext(context.Background(), engineMCPContext{})
	actual, err := finalizeBeforeCompletion(ctx, output, nil)
	if err == nil {
		_, err = finalizeAfterCompletion(ctx, actual)
	}
	if !errors.Is(err, wantErr) || !reflect.DeepEqual(output.order, []string{"finalize", "mcp"}) {
		t.Fatalf("unexpected MCP finalizer failure: order=%v err=%v", output.order, err)
	}
}
