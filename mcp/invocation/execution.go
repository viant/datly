package invocation

import (
	"github.com/viant/datly/exec"
	structjson "github.com/viant/structology/encoding/json"
	"net/http"

	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
)

// Execution is the protocol-neutral outcome of one exact MCP component call.
type Execution struct {
	selection exec.OutputFieldFilter
	value     interface{}
	err       error
	context   *xexec.Context
}

func (e *Execution) Value() interface{} {
	if e == nil {
		return nil
	}
	return e.value
}

func (e *Execution) Error() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *Execution) StatusCode() int {
	if e == nil {
		return http.StatusInternalServerError
	}
	if code := response.ErrorStatusCode(e.err, 0); code != 0 {
		return code
	}
	if e.context != nil && e.context.StatusCode != 0 {
		return e.context.StatusCode
	}
	if value, ok := e.value.(response.StatusCoder); ok && value.StatusCode() != 0 {
		return value.StatusCode()
	}
	if e.err != nil {
		return response.ErrorStatusCode(e.err, http.StatusInternalServerError)
	}
	return http.StatusOK
}

func (e *Execution) Payload() ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	if _, raw := e.value.(response.Response); !raw && e.selection != nil {
		return structjson.MarshalStandard(e.value, structjson.WithPathFieldExcluder(e.selection))
	}
	return encodePayload(e.value)
}
