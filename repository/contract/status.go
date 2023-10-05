package contract

import (
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/xdatly/handler/response"
)

// TODO move it to some other package
func StatusSuccess(state *expand.State) response.Status {
	status := response.Status{Status: "ok"}
	if state != nil {
		status.Extras = state.ResponseBuilder.Content
	}

	return status
}
