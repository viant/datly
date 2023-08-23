package locator

import (
	"context"
	"github.com/viant/datly/view/state/kind"
	"os"
)

type Env struct {
	env map[string]string
}

func (v *Env) Names() []string {
	return os.Environ()
}

func (v *Env) Value(ctx context.Context, name string) (interface{}, bool, error) {
	ret, ok := v.env[name]
	return ret, ok, nil
}

// NewEnv returns env locator
func NewEnv(_ ...Option) (kind.Locator, error) {
	ret := &Env{env: make(map[string]string)}
	for _, k := range os.Environ() {
		ret.env[k] = os.Getenv(k)
	}
	return ret, nil
}
