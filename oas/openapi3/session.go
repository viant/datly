package openapi3

import (
	"context"
	"fmt"
	"strings"
)

type sessionKey string

var _sessionKey = sessionKey("session")

type (
	Session struct {
		defers     []func() error
		components map[string]*Components
		Location   string
	}
)


func (s *Session) AddDefer(fn func() error) {
	s.defers = append(s.defers, fn)
}

//RegisterComponents returns location components
func (s *Session) RegisterComponents(location string, components *Components) {

	if len(components.Schemas) == 0 {
		components.Schemas = map[string]*Schema{}
	}
	if len(components.Parameters) == 0 {
		components.Parameters = map[string]*Parameter{}
	}
	if len(components.Headers) == 0 {
		components.Headers = map[string]*Parameter{}
	}
	if len(components.RequestBodies) == 0 {
		components.RequestBodies = map[string]*RequestBody{}
	}
	if len(components.Responses) == 0 {
		components.Responses = map[string]*Response{}
	}
	if len(components.SecuritySchemes) == 0 {
		components.SecuritySchemes = map[string]*SecurityScheme{}
	}
	if len(components.Examples) == 0 {
		components.Examples = map[string]*Example{}
	}
	if len(components.Links) == 0 {
		components.Links = map[string]*Link{}
	}
	if len(components.Callbacks) == 0 {
		components.Callbacks = Callbacks{}
	}
	if len(s.components) == 0 {
		s.components = map[string]*Components{}
	}
	s.components[location] = components
}

//LookupSchema lookups schema
func (s *Session) LookupSchema(location string, ref string) (*Schema, error) {
	switch ref[0] {
	case '#':
		id := s.normalizeRef(ref[1:], "/components/schemas/")
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Schemas[id]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}


//LookupParameter lookup parameters
func (s *Session) LookupParameter(location string, ref string) (*Parameter, error) {
	switch ref[0] {
	case '#':
		id := s.normalizeRef(ref[1:], "/components/parameters/")
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Parameters[id]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupHeaders lookup headers
func (s *Session) LookupHeaders(location string, ref string) (*Parameter, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Headers[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupRequestBody lookup request body
func (s *Session) LookupRequestBody(location string, ref string) (*RequestBody, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.RequestBodies[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupResponse lookup response
func (s *Session) LookupResponse(location string, ref string) (*Response, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Responses[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupSecurityScheme lookup security scheme
func (s *Session) LookupSecurityScheme(location string, ref string) (*SecurityScheme, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.SecuritySchemes[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupExample lookup example
func (s *Session) LookupExample(location string, ref string) (*Example, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Examples[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}



//LookupLink lookup link
func (s *Session) LookupLink(location string, ref string) (*Link, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Links[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}

//LookupLink lookup callback
func (s *Session) LookupCallback(location string, ref string) (*CallbackRef, error) {
	switch ref[0] {
	case '#':
		components, ok := s.components[location]
		if !ok {
			return nil, fmt.Errorf("failed to lookup location: %v", location)
		}
		value, ok := components.Callbacks[ref[1:]]
		if !ok {
			return nil, fmt.Errorf("failed to lookup %v, at %v", ref, location)
		}
		result := *value
		result.Ref = ref
		return &result, nil
	case '.':

	case '/':

	default:

	}
	return nil, fmt.Errorf("unsupported: %v, at %v", ref, location)
}


func (s *Session) Close() error {
	if len(s.defers) > 0 {
		for i := range s.defers {
			if err := s.defers[i](); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) normalizeRef(ref string, scope string) string {
	return strings.Replace(ref, scope, "", 1)
}

func NewSession() *Session {
	return &Session{}
}

//LookupSession lookup session
func LookupSession(ctx context.Context) *Session {
	value := ctx.Value(_sessionKey)
	if value == nil {
		return nil
	}
	return value.(*Session)
}



//NewSessionContext creates a session context
func NewSessionContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, _sessionKey, NewSession())
}
