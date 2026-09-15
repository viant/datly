package exec

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/xdatly/response"
	"strings"
	"testing"
)

func TestPanicDiagnosticsStayPrivate(t *testing.T) {
	cause := &response.Error{Code: 400, Payload: "PRIVATE payload", Cause: errors.New("PRIVATE cause")}
	var err error
	func() {
		defer func() {
			if v := recover(); v != nil {
				err = NewPanicError("test recovery", v)
			}
		}()
		panic(cause)
	}()
	var recovered *PanicError
	if !errors.As(err, &recovered) || recovered.Cause() != cause || !strings.Contains(string(recovered.Stack()), "TestPanicDiagnosticsStayPrivate") {
		t.Fatal("missing recovery diagnostics")
	}
	if _, ok := response.ErrorBody(err); ok {
		t.Fatal("panic acquired public error contract")
	}
	if strings.Contains(err.Error(), "PRIVATE") || !strings.Contains(fmt.Sprintf("%+v", recovered), "PRIVATE") {
		t.Fatal("diagnostic formatting")
	}
	encoded, _ := json.Marshal(err)
	if strings.Contains(string(encoded), "PRIVATE") {
		t.Fatal("serialized diagnostics")
	}
	stack := recovered.Stack()
	stack[0] = 0
	if recovered.Stack()[0] == 0 {
		t.Fatal("stack mutation")
	}
}
