package component

import (
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/xdatly/handler/response"
)

// TODO move it to some other package
func StatusSuccess(state *expand2.State) response.Status {
	status := response.Status{Status: "ok"}
	if state != nil {
		status.Extras = state.ResponseBuilder.Content
	}

	return status
}
