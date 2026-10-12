package command_test

import (
	"bytes"
	"context"
	"github.com/viant/datly/cmd/command"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"strings"
	"testing"
)

func TestCommandRejectsInvalidMethodMismatchBeforeListener(t *testing.T) {
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) { c["MethodMismatchPolicy"] = "invalid" })
	var out, diagnostics bytes.Buffer
	code := (command.Service{}).Run(context.Background(), []string{"run", "-conf", f.Config}, &out, &diagnostics)
	if code != 1 || strings.Contains(out.String(), "listening on") {
		t.Fatal(code, out.String(), diagnostics.String())
	}
}
