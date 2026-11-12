package session

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/datly/repository"
	"reflect"
	"strconv"
	"sync"

	"github.com/viant/bindly"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
)

// ContractBinder exposes native locator binding to metadata hooks. It does not
// change Session.Bind or invoke the destination's business initializer. Operation
// values come from the registered target route, never the metadata HTTP request.
func (s *Session) ContractBinder(method, path string) bindly.Binder {
	return &contractBinder{session: s, method: method, path: path}
}

type contractBinder struct {
	session      *Session
	method, path string
	mu           sync.Mutex
	scopes       map[reflect.Type]*Session
}

func (b *contractBinder) Bind(ctx context.Context, target any, options ...bindly.BindOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(options) != 0 {
		return fmt.Errorf("v0 contract binding does not support BindOptions")
	}
	value := reflect.ValueOf(target)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("contract state must be a nonnil pointer to a struct")
	}
	if b.session == nil {
		return fmt.Errorf("contract binding session is required")
	}
	// Values are shared only within this request and metadata state type. This
	// avoids repeated auth-component work across nested policies without allowing
	// same-named fields in unrelated state types or business input to collide.
	scoped := b.scope(value.Elem().Type())
	opts := scoped.Indirect(true)
	if scoped.view != nil && scoped.state != nil {
		opts = scoped.ViewOptions(scoped.view).Indirect(true)
	}
	lookup := extension.Config.Types.Lookup
	if scoped.resource != nil {
		lookup = scoped.resource.LookupType()
	}
	var fs *embed.FS
	if embedded, ok := target.(state.Embedder); ok {
		fs = embedded.EmbedFS()
	}
	targetState := structology.NewStateType(value.Elem().Type()).WithValue(target)
	var parameters state.Parameters
	for i := 0; i < value.Elem().NumField(); i++ {
		field := value.Elem().Type().Field(i)
		if field.PkgPath != "" {
			continue
		}
		if _, ok := field.Tag.Lookup("parameter"); !ok {
			if tag, ok := field.Tag.Lookup("bind"); ok {
				field.Tag = reflect.StructTag(string(field.Tag) + " parameter:" + strconv.Quote(tag))
			}
		}
		parameter, err := state.BuildParameter(&field, fs, lookup)
		if err != nil {
			return err
		}
		if parameter == nil {
			continue
		}
		if parameter.In.Kind == "contract" {
			var operation string
			switch parameter.In.Name {
			case "method":
				operation = b.method
			case "path":
				operation = b.path
			default:
				return fmt.Errorf("unknown contract operation value %q", parameter.In.Name)
			}
			if field.Type.Kind() != reflect.String {
				return fmt.Errorf("contract operation field %s must be a string", field.Name)
			}
			value.Elem().Field(i).SetString(operation)
			continue
		}
		parameters = append(parameters, parameter)
	}
	return scoped.SetState(ctx, parameters, targetState, opts)
}

// ApplyContract selects metadata for ordinary binding without changing its Binder
// interface. Rebuild the request-body locator so metadata state cannot predecode
// and retain a less restrictive business body.
func (s *Session) ApplyContract(effective *repository.EffectiveContract, unmarshal func([]byte, interface{}) error) {
	s.contractInput = effective.Input
	if effective.Body == nil || !effective.Body.Changed() {
		return
	}
	s.locatorOptions = append(append([]locator.Option(nil), s.locatorOptions...), locator.WithUnmarshal(unmarshal))
	s.locatorOpt = locator.NewOptions(s.locatorOptions)
	s.kindLocator = locator.NewKindsLocator(nil, s.locatorOptions...)
}

func (b *contractBinder) scope(typ reflect.Type) *Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	if scoped := b.scopes[typ]; scoped != nil {
		return scoped
	}
	scoped := &Session{cache: newCache(), views: b.session.views, view: b.session.view, Options: b.session.Options, Types: *state.NewTypes()}
	scoped.contractInput = nil
	scoped.locatorOptions = append([]locator.Option(nil), scoped.locatorOptions...)
	if b.scopes == nil {
		b.scopes = map[reflect.Type]*Session{}
	}
	b.scopes[typ] = scoped
	return scoped
}
