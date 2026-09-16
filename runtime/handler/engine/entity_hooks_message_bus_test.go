package engine

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/mutation"
	"github.com/viant/xdatly/mbus"
)

type messageHookInput struct{ Value int }
type messageHookEntity struct{ ID, Value int }

type notifyingEntityHooks struct {
	Input     *messageHookInput   `bind:"kind=input,required"`
	Bus       xhandler.MessageBus `bind:"kind=mbus,required"`
	Data      xhandler.Data       `bind:"kind=data,required"`
	pending   *messageHookEntity
	finalized int
}

var _ xhandler.EntityHooks[messageHookEntity, xhandler.NoParent, messageHookEntity] = (*notifyingEntityHooks)(nil)
var _ mutation.Finalizer[messageHookInput, messageHookEntity] = (*notifyingEntityHooks)(nil)

func (h *notifyingEntityHooks) Init(_ context.Context, entity *messageHookEntity, _ xhandler.LifecycleContext[messageHookEntity, xhandler.NoParent, messageHookEntity]) error {
	entity.Value = h.Input.Value
	return nil
}

func (h *notifyingEntityHooks) Validate(_ context.Context, entity *messageHookEntity, _ xhandler.LifecycleContext[messageHookEntity, xhandler.NoParent, messageHookEntity]) error {
	// Application policy decides whether this mutation warrants a message.
	if entity.Value >= 10 {
		copy := *entity
		h.pending = &copy
	}
	return nil
}

func (h *notifyingEntityHooks) Finalize(ctx context.Context, _ *messageHookInput, _ *messageHookEntity, outcome xhandler.Outcome) error {
	h.finalized++
	if !outcome.CommitConfirmed() || h.pending == nil {
		return nil
	}
	_, err := h.Bus.Push(ctx, h.Bus.Message("records.changed", *h.pending))
	return err
}

type committedRecordBus struct {
	db       *sql.DB
	messages []*mbus.Message
}

func (b *committedRecordBus) Message(destination string, value any, options ...mbus.Option) *mbus.Message {
	message := &mbus.Message{Resource: destination, Data: value}
	mbus.Options(options).Apply(message)
	return message
}

func (b *committedRecordBus) Push(ctx context.Context, message *mbus.Message) (*mbus.Confirmation, error) {
	// Read through the database, not the mutation's transaction: publication
	// must see an already committed row with the exact message payload.
	payload, ok := message.Data.(messageHookEntity)
	if !ok {
		return nil, fmt.Errorf("unexpected message payload %T", message.Data)
	}
	var value int
	if err := b.db.QueryRowContext(ctx, "SELECT value FROM records WHERE id=?", payload.ID).Scan(&value); err != nil {
		return nil, err
	}
	if value != payload.Value {
		return nil, fmt.Errorf("message differs from committed row")
	}
	b.messages = append(b.messages, message)
	return &mbus.Confirmation{}, nil
}

// One integration matrix exercises typed hook DI, business policy and the real
// Data/engine completion boundary; it does not contact an external message bus.
func TestMutationHookMessageBusPublishesAfterCommitSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		value                  int
		callerTx               bool
		wantError              bool
		wantRows, wantMessages int
	}{
		{"business event after commit", 10, false, false, 1, 1},
		{"business policy needs no event", 1, false, false, 1, 0},
		{"SQL failure rolls back", 20, false, true, 0, 0},
		{"caller transaction is still pending", 10, true, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,value INTEGER CHECK(value != 20))"); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			if tc.callerTx {
				var err error
				tx, err = db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			input := &messageHookInput{Value: tc.value}
			bus := &committedRecordBus{db: db.DB}
			hooks := &notifyingEntityHooks{}
			adapter := &outcomeAwareHandler{
				execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					if err := invocation.Binder.Bind(ctx, hooks); err != nil {
						return nil, err
					}
					if hooks.Input != input || hooks.Bus != bus || hooks.Data == nil {
						return nil, fmt.Errorf("wrong scoped hook dependencies")
					}
					entity := &messageHookEntity{ID: 1}
					state := xhandler.LifecycleContext[messageHookEntity, xhandler.NoParent, messageHookEntity]{Output: entity}
					if err := hooks.Init(ctx, entity, state); err != nil {
						return nil, err
					}
					if err := hooks.Validate(ctx, entity, state); err != nil {
						return nil, err
					}
					if err := hooks.Data.Insert("records", entity); err != nil {
						return nil, err
					}
					if len(bus.messages) != 0 {
						return nil, fmt.Errorf("message sent before transaction completion")
					}
					return entity, nil
				},
				finalize: func(ctx context.Context, _ rhandler.Invocation, result any, outcome xhandler.Outcome) error {
					entity, _ := result.(*messageHookEntity)
					return hooks.Finalize(ctx, input, entity, outcome)
				},
			}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[messageHookInput]()), BoundInput: input, DataSource: sqldml.Source{DB: db.DB, Tx: tx}, Capabilities: xhandler.Capabilities{MessageBus: bus}, Handler: adapter})
			if (err != nil) != tc.wantError {
				t.Fatalf("execution error=%v", err)
			}
			if hooks.finalized != 1 || len(bus.messages) != tc.wantMessages {
				t.Fatalf("finalizations=%d messages=%d", hooks.finalized, len(bus.messages))
			}
			if len(bus.messages) == 1 && (bus.messages[0].Resource != "records.changed" || bus.messages[0].Data != (messageHookEntity{ID: 1, Value: tc.value})) {
				t.Fatalf("message=%+v", bus.messages[0])
			}
			if tx != nil {
				var rows int
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 1 {
					t.Fatalf("caller pending rows=%d error=%v", rows, err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("caller lost transaction ownership: %v", err)
				}
			}
			db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct{ Total int }{{tc.wantRows}})
		})
	}
}
