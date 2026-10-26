package standalone

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/loggerapp"
	"github.com/viant/datly/standalone/testdata/loggerapp/components"
	h "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/mbus"
)

type hostMessageBus struct{ messages []*mbus.Message }

func (b *hostMessageBus) Message(dest string, data any, options ...mbus.Option) *mbus.Message {
	m := &mbus.Message{Resource: dest, Data: data}
	mbus.Options(options).Apply(m)
	return m
}
func (b *hostMessageBus) Push(_ context.Context, m *mbus.Message) (*mbus.Confirmation, error) {
	b.messages = append(b.messages, m)
	return &mbus.Confirmation{MessageID: "host-message"}, nil
}
func TestHostInvocationMessageBusBindingAndProtection(t *testing.T) {
	for _, mode := range []string{"host", "missing", "provider"} {
		t.Run(mode, func(t *testing.T) {
			f := fixture.New(t)
			cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
			if err != nil {
				t.Fatal(err)
			}
			bus := new(hostMessageBus)
			options := Options{Config: cfg, Holders: []any{components.MbusComponent{}}, RequireLinked: true}
			if mode == "host" {
				options.InvocationMessageBus = bus
			}
			if mode == "provider" {
				options.Providers = []locator.Provider{provider.Static(h.MessageBusKey, bus)}
			}
			server, err := New(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(context.Background())
			if err = server.Reload(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("POST", "/publish", strings.NewReader(`{"body":"source payload"}`))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.manager.ServeHTTP(response, req)
			if mode == "host" {
				if response.Code != 200 || len(bus.messages) != 1 || bus.messages[0].Resource != "records" || bus.messages[0].Data != "source payload" {
					t.Fatalf("code=%d body=%s messages=%v", response.Code, response.Body, bus.messages)
				}
			} else {
				if response.Code < 400 || len(bus.messages) != 0 {
					t.Fatalf("untrusted or missing bus accepted: %d %s", response.Code, response.Body)
				}
			}
		})
	}
}
