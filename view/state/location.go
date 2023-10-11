package state

import (
	"fmt"
	"os"
	"strings"
)

// Location represents parameter location
type Location struct {
	Kind Kind   `json:",omitempty" yaml:"Kind"`
	Name string `json:",omitempty" yaml:"Name"`
}

// Validate checks if Location is valid
func (l *Location) Validate() error {
	if err := l.Kind.Validate(); err != nil {
		return err
	}

	if err := ParamName(l.Name).Validate(l.Kind); err != nil {
		return fmt.Errorf("unsupported param name %w", err)
	}

	return nil
}

// ParamName represents name of parameter in given Location.Kind
// i.e. if you want to extract lang from query string: ?foo=bar&lang=eng
// required Kind is KindQuery and ParamName `lang`
type ParamName string

// Validate checks if ParamName is valid
func (p ParamName) Validate(kind Kind) error {
	switch kind {
	case KindObject:
		split := strings.Split(string(p), ",")
		if len(split) == 0 {
			return fmt.Errorf("param name can't be empty")
		}

		if len(split) == 1 {
			return fmt.Errorf("param Object must contain at least 2 params")
		}

		return nil

	case KindRequest, KindLiteral, KindConst, KindRequestBody, KindQuery:
		return nil
	case KindDataView, KindPath, KindHeader, KindRepeated, KindCookie, KindParam, KindState, KindContext, KindOutput, KindComponent:
		if p == "" {
			return fmt.Errorf("param name can't be empty")
		}

		return nil
	case KindEnvironment:
		if os.Getenv(string(p)) == "" {
			return fmt.Errorf("env variable %s not set", p)
		}
		return nil
	}
	return fmt.Errorf("unsupported param name %v for location kind %v", p, kind)
}

// NewHeaderLocation creates a query location
func NewHeaderLocation(name string) *Location {
	return &Location{Name: name, Kind: KindHeader}
}

// NewQueryLocation creates a query location
func NewQueryLocation(name string) *Location {
	return &Location{Name: name, Kind: KindQuery}
}

// NewBodyLocation creates a body location
func NewBodyLocation(name string) *Location {
	return &Location{Name: name, Kind: KindRequestBody}
}

// NewRequestLocation creates a body location
func NewRequestLocation() *Location {
	return &Location{Name: "", Kind: KindRequest}
}

// NewOutputLocation creates an output location
func NewOutputLocation(name string) *Location {
	return &Location{Name: name, Kind: KindOutput}
}

// NewObjectLocation creates an output location
func NewObjectLocation(name string) *Location {
	return &Location{Name: name, Kind: KindObject}
}

// NewDataViewLocation creates a dataview location
func NewDataViewLocation(name string) *Location {
	return &Location{Name: name, Kind: KindDataView}
}

func NewConstLocation(name string) *Location {
	return &Location{Kind: KindConst, Name: name}
}

// NewPathLocation creates a path location
func NewPathLocation(name string) *Location {
	return &Location{Name: name, Kind: KindPath}
}

// NewParameterLocation creates a parameter location
func NewParameterLocation(name string) *Location {
	return &Location{Name: name, Kind: KindParam}
}

// NewState creates a state location
func NewState(name string) *Location {
	return &Location{Name: name, Kind: KindState}
}

// NewComponent creates a component location
func NewComponent(name string) *Location {
	return &Location{Name: name, Kind: KindComponent}
}

// NewContextLocation creates a context location
func NewContextLocation(name string) *Location {
	return &Location{Name: name, Kind: KindContext}
}
