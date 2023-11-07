package session

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

type cache struct {
	values        map[string]interface{}
	parameterLock map[string]sync.Locker
	sync.RWMutex
}

func (c *cache) lookup(parameter *state.Parameter) (interface{}, bool) {
	c.RWMutex.RLock()
	ret, ok := c.values[c.key(parameter)]
	c.RWMutex.RUnlock()
	return ret, ok
}

func (s *Session) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.cache.values)
}

func (s *Session) Unmarshal(parameters state.Parameters, data []byte) error {
	err := json.Unmarshal(data, &s.cache.values)
	if err != nil {
		return err
	}
	for _, parameter := range parameters {
		value, ok := s.cache.values[parameter.Name]
		if !ok {
			continue
		}
		parameterType := parameter.OutputType()
		switch parameterType.Kind() {
		case reflect.Slice:
			switch parameterType.Elem().Kind() {
			case reflect.Struct:
				if s.cache.values[parameter.Name], err = s.unmarshalSliceParameter(value, parameter, parameterType); err != nil {
					return fmt.Errorf("failed to umarshal slice parameter: %s, %w", parameter.Name, err)
				}
			}
			continue
		case reflect.Ptr:
			switch parameterType.Elem().Kind() {
			case reflect.Struct:
				if s.cache.values[parameter.Name], err = s.umarshalStructParameter(parameter.Name, parameter.OutputType(), value); err != nil {
					return fmt.Errorf("failed to umarshal struct parameter: %s, %w", parameter.Name, err)
				}
			}
		case reflect.Struct:
			if s.cache.values[parameter.Name], err = s.umarshalStructParameter(parameter.Name, parameter.OutputType(), value); err != nil {
				return fmt.Errorf("failed to umarshal struct parameter: %s, %w", parameter.Name, err)
			}
		}
	}

	return nil
}

func (s *Session) unmarshalSliceParameter(value interface{}, parameter *state.Parameter, parameterType reflect.Type) (interface{}, error) {
	var err error
	values, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected: %T, but had: %T", values, value)
	}
	xSlice := xunsafe.NewSlice(parameterType.Elem())
	slicePtrValue := reflect.New(xSlice.Type)
	slicePtr := xunsafe.ValuePointer(&slicePtrValue)
	for i, item := range values {
		if values[i], err = s.umarshalStructParameter(parameter.Name, parameterType, item); err != nil {
			return nil, err
		}
	}
	appender := xSlice.Appender(slicePtr)
	appender.Append(values...)
	return slicePtrValue.Elem().Interface(), nil
}

func (s *Session) umarshalStructParameter(name string, parameterType reflect.Type, value interface{}) (interface{}, error) {
	isPtr := false
	if parameterType.Kind() == reflect.Ptr {
		parameterType = parameterType.Elem()
		isPtr = true
	}
	sValue := reflect.New(parameterType)
	parameterValue := sValue.Interface()
	fieldData, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed unmarsha: %w", err)
	}
	if err = json.Unmarshal(fieldData, parameterValue); err != nil {
		return nil, fmt.Errorf("failed to transfer %v %w", name, err)
	}
	if isPtr {
		return parameterValue, nil
	}
	return sValue.Elem().Interface(), nil
}

func (c *cache) lockParameter(parameter *state.Parameter) sync.Locker {
	c.RWMutex.Lock()
	ret, ok := c.parameterLock[c.key(parameter)]
	if !ok {
		ret = &sync.Mutex{}
		c.parameterLock[c.key(parameter)] = ret
	}
	c.RWMutex.Unlock()
	return ret
}

func (c *cache) put(parameter *state.Parameter, value interface{}) {
	c.RWMutex.Lock()
	c.values[c.key(parameter)] = value
	c.RWMutex.Unlock()
}

func (c *cache) key(parameter *state.Parameter) string {
	ret := parameter.Name
	return ret
}

func newCache() *cache {
	return &cache{values: make(map[string]interface{}), parameterLock: make(map[string]sync.Locker)}
}
