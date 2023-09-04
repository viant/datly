package component

import (
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/xdatly/handler/response"
)

// TODO move it to some other package
func StatusSuccess(state *expand.State) response.ResponseStatus {
	status := response.ResponseStatus{Status: "ok"}
	if state != nil {
		status.Extras = state.ResponseBuilder.Content
	}

	return status
}
