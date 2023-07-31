package state

import (
	"fmt"
	"os"
)

type (
	Location struct {
		Kind Kind   `json:",omitempty"`
		Name string `json:",omitempty"`
	}
)

// Validate checks if ParamName is valid
func (l *Location) Validate() error {
	if err := l.Kind.Validate(); err != nil {
		return err
	}
	switch l.Kind {
	case KindRequest, KindLiteral, KindRequestBody:
		return nil
	case KindDataView, KindPath, KindQuery, KindHeader, KindCookie, KindParam:
		if l.Name == "" {
			return fmt.Errorf("param name can't be empty")
		}

		return nil
	case KindEnvironment:
		if os.Getenv(l.Name) == "" {
			return fmt.Errorf("env variable %s not set", l.Name)
		}
		return nil
	}
	return fmt.Errorf("unsupported param name %v for location kind %v", l.Name, l.Kind)
}
