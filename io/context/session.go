package context

import (
	"context"
	"net/http"
	"reflect"
	"sync"
)

//Session represent user session data
type (
	Input struct {
		Header      http.Header
		URIParams   map[string]string
		QueryString map[string]string
		Body        map[string]string
		DataView    map[string]string
	}

	Session struct {
		Input      map[string]interface{}
		Data       map[string]string
		Maps      map[string]*Map
		MultiMaps map[string]*MultiMap
		sync.RWMutex
	}
)

//NewMap creates a map
func (s *Session) NewMap(aType reflect.Type, name string, fn func(instance interface{}) interface{}) *Map {
	s.Lock()
	defer s.Unlock()
	s.Maps[name] = NewMap(aType, fn)
	return s.Maps[name]
}

//NewMultiMap creates a Multimap
func (s *Session) NewMultiMap(aType reflect.Type, name string, fn func(instance interface{}) interface{}) *MultiMap {
	s.Lock()
	defer s.Unlock()
	s.MultiMaps[name] = NewMultiMap(aType, fn)
	return s.MultiMaps[name]
}

//NewSession creates a new session
func NewSession() *Session {
	return &Session{
		MultiMaps: make(map[string]*MultiMap),
		Maps:      make(map[string]*Map),
		Input:     make(map[string]interface{}),
	}
}

//dataPoolTypeKey defines context data pool key
type sessionTypeKey string

var _sessionTypeKey = sessionTypeKey("session")

//WithSession returns new context with session
func WithSession(parent context.Context, sess *Session) context.Context {
	return context.WithValue(parent, _sessionTypeKey, sess)
}

//LookupSession returns session or nil
func LookupSession(ctx context.Context) *Session {
	value := ctx.Value(_sessionTypeKey)
	if value == nil {
		return nil
	}
	result, ok := value.(*Session)
	if !ok { //sanity check: this should never happen
		return nil
	}
	return result
}
