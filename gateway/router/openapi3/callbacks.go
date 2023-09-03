package openapi3

import "context"

type (
	Callbacks   map[string]*CallbackRef
	CallbackRef struct {
		Ref string `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Callback   `yaml:",inline"`
	}
	Callback map[string]*PathItem
)


func (s *CallbackRef) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp CallbackRef
	tmp := temp(*s)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupCallback(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}