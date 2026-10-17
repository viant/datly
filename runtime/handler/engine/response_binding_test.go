package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/responsebinding"
	rh "github.com/viant/datly/runtime/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	legacy "github.com/viant/xdatly/handler/response"
)

type projectedBusinessOutput struct {
	cause           error
	calls, mcpCalls *int
}

func (o *projectedBusinessOutput) Finalize(_ context.Context, incoming error) error {
	*o.calls++
	if incoming != nil {
		return incoming
	}
	return o.cause
}
func (o *projectedBusinessOutput) FinalizeMCP(context.Context, xmcp.Context) error {
	*o.mcpCalls++
	return errors.New("modeled error response ran success MCP hook")
}

func TestResponseProjectionPrecedesActivityFailurePublication(t *testing.T) {
	for _, mode := range []string{"project", "ordinary invoke", "projection failure", "latched rejection", "projection panic", "success"} {
		t.Run(mode, func(t *testing.T) {
			ctx := dexec.CaptureOutputSelection(xmcp.WithContext(t.Context(), engineMCPContext{}))
			root := neutralDataScope(ctx)
			if err := root.enrollBufferedScope(ctx); err != nil {
				t.Fatal(err)
			}
			// A published managed unit intentionally makes ordinary retirement
			// unavailable. Classification must precede completion, not erase a failure.
			if _, err := root.databaseUnit(ctx, &staticDataSource{}, "session-writer", ""); err != nil {
				t.Fatal(err)
			}
			if root.neutralReaderComposition() {
				t.Fatal("expected published writer")
			}
			ctx = PrepareComponent(withDataScope(ctx, root), ComponentBufferedImperative, "")
			input := testRouteInput(t, reflect.TypeFor[struct{}]())
			typ := reflect.TypeFor[projectedBusinessOutput]()
			candidate := &legacy.Error{Code: 403, Message: "hidden requested record"}
			projectionFailure := errors.New("declared output projection failed")
			protectedFailure := errors.New("earlier protected rejection")
			calls, mcpCalls, projections := 0, 0, 0
			output := &projectedBusinessOutput{cause: candidate, calls: &calls, mcpCalls: &mcpCalls}
			if mode == "success" {
				output.cause = nil
			}
			fresh := &projectedBusinessOutput{calls: &calls, mcpCalls: &mcpCalls}
			var token *responsebinding.Token
			if mode != "ordinary invoke" {
				ctx, token = responsebinding.New(ctx, root, input.Route().String(), typ, func(e *legacy.Error) (any, bool, error) {
					projections++
					if e != candidate {
						t.Fatal("raw identity lost")
					}
					if drainowner.ProtectedFailure(root.nativeInvocation) != nil {
						t.Fatal("candidate already published before projection")
					}
					if !errors.Is(drainowner.CheckActivities(root.nativeInvocation), drainowner.ErrActivityUnfinished) {
						t.Fatal("projection ran after activity completion")
					}
					if mode == "projection failure" {
						return nil, false, projectionFailure
					}
					if mode == "projection panic" {
						panic("projection panic")
					}
					return fresh, true, nil
				})
			}
			if mode == "latched rejection" {
				drainowner.FailProtected(root.nativeInvocation, protectedFailure)
			}
			actual, err := New().Execute(ctx, Request{CanonicalReader: true, BufferedComponentCalls: true, Input: input, OutputType: typ, Handler: rh.HandlerFunc(func(call context.Context, _ rh.Invocation) (any, error) {
				dexec.PublishOutputSelection(call, output, responseProjectionFilter{})
				return output, nil
			})})
			wantCalls := 1
			if mode == "latched rejection" {
				wantCalls = 0
			}
			if calls != wantCalls || mcpCalls != 0 {
				t.Fatalf("finalize calls %d MCP calls %d", calls, mcpCalls)
			}
			if mode == "project" {
				if err != nil || actual != fresh || projections != 1 || token.Consume() != candidate || drainowner.ProtectedFailure(root.nativeInvocation) != nil || len(root.finalizers) != 0 {
					t.Fatalf("projection failed: actual %T error %v projections %d queued hooks %d", actual, err, projections, len(root.finalizers))
				}
				if dexec.SelectedOutputFields(ctx, fresh) != nil {
					t.Fatal("stale successful row selection leaked into error response")
				}
				if err := drainowner.CheckActivities(root.nativeInvocation); err != nil {
					t.Fatalf("modeled response left failed activity: %v", err)
				}
			} else if mode == "success" {
				if err != nil || actual != output || token.Consume() != nil || projections != 0 || dexec.SelectedOutputFields(ctx, output) == nil {
					t.Fatalf("ordinary successful selection lost: err %v projections %d", err, projections)
				}
			} else {
				if err == nil || token.Consume() != nil || drainowner.ProtectedFailure(root.nativeInvocation) == nil {
					t.Fatalf("failure cleared: %v", err)
				}
				if mode == "projection failure" && (!errors.Is(err, candidate) || !errors.Is(err, projectionFailure)) {
					t.Fatalf("projection failure lost original candidate: %v", err)
				}
				if mode == "latched rejection" && projections != 0 {
					t.Fatal("projected with preexisting protected rejection")
				}
			}
		})
	}
}

type responseProjectionFilter struct{}

func (responseProjectionFilter) ExcludePath([]string, string) bool { return true }
