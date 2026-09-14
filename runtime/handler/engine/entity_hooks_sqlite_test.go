package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/mbus"
)

type hookLogger struct{ events []string }

func (l *hookLogger) Debug(message string, _ ...any) { l.events = append(l.events, message) }
func (*hookLogger) Info(string, ...any)              {}
func (*hookLogger) Warn(string, ...any)              {}
func (*hookLogger) Error(string, ...any)             {}

type hookBus struct{ pushes int }

func (b *hookBus) Push(context.Context, *mbus.Message) (*mbus.Confirmation, error) {
	b.pushes++
	return &mbus.Confirmation{}, nil
}
func (*hookBus) Message(dest string, data any, options ...mbus.Option) *mbus.Message {
	message := &mbus.Message{Resource: dest, Data: data}
	mbus.Options(options).Apply(message)
	return message
}

type hookEntity struct{ ID, Value int }
type hookParent struct{ Minimum int }
type hookInput struct{ Value int }
type injectedEntityHooks struct {
	Input       *hookInput                  `bind:"kind=input,required"`
	Logger      xhandler.Logger             `bind:"kind=logger,required"`
	Bus         xhandler.MessageBus         `bind:"kind=mbus,required"`
	Starter     xhandler.TransactionStarter `bind:"kind=transactionStarter,required"`
	initialized bool
}

func (h *injectedEntityHooks) Init(_ context.Context, entity *hookEntity, _ xhandler.EntityState[hookEntity, hookParent]) error {
	entity.Value = h.Input.Value
	h.initialized = true
	h.Logger.Debug("init")
	return nil
}
func (h *injectedEntityHooks) Validate(_ context.Context, entity *hookEntity, state xhandler.EntityState[hookEntity, hookParent]) error {
	if !h.initialized {
		return fmt.Errorf("hook state was not retained")
	}
	h.Logger.Debug("validate")
	if entity.Value < state.Parent.Minimum {
		return fmt.Errorf("value below minimum")
	}
	return nil
}

// This verifies the public typed hook contracts and existing DI/Data owners,
// not the still-in-progress generated mutation orchestrator.
func TestEntityHooksUseInvocationCapabilitiesSQLite(t *testing.T) {
	for _, value := range []int{0, 3} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,value INTEGER)"); err != nil {
				t.Fatal(err)
			}
			logger, bus := &hookLogger{}, &hookBus{}
			input := &hookInput{Value: value}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(hookInput{})), BoundInput: input,
				Capabilities: xhandler.Capabilities{Logger: logger, MessageBus: bus}, DataSource: sqldml.Source{DB: db.DB},
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					hooks := &injectedEntityHooks{}
					if err := invocation.Binder.Bind(ctx, hooks); err != nil {
						return nil, err
					}
					if hooks.Input != input || hooks.Logger != logger || hooks.Bus != bus {
						return nil, fmt.Errorf("wrong scoped dependencies")
					}
					var lifecycle xhandler.EntityHooks[hookEntity, hookParent] = hooks
					entity := &hookEntity{ID: 1}
					state := xhandler.EntityState[hookEntity, hookParent]{Parent: &hookParent{Minimum: 1}}
					if err := lifecycle.Init(ctx, entity, state); err != nil {
						return nil, err
					}
					if err := lifecycle.Validate(ctx, entity, state); err != nil {
						return nil, err
					}
					if err := hooks.Starter.Start(ctx); err != nil {
						return nil, err
					}
					data, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil {
						return nil, err
					}
					if !found {
						return nil, fmt.Errorf("missing DML")
					}
					return entity, data.(xhandler.DML).Insert("records", entity)
				}),
			})
			if (err != nil) != (value == 0) {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(logger.events, []string{"init", "validate"}) || bus.pushes != 0 {
				t.Fatalf("events=%v premature bus pushes=%d", logger.events, bus.pushes)
			}
			want := []hookEntity{}
			if value != 0 {
				want = append(want, hookEntity{ID: 1, Value: value})
			}
			db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,value FROM records"}, want)
		})
	}
}
