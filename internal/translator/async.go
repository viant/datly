package translator

import (
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router"
	rasync "github.com/viant/datly/repository/async"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler/async"
	"strings"
)

func (s *Service) applyAsyncOption(resource *Resource, route *router.Route) error {
	asyncModule := resource.Rule.Async

	if len(resource.AsyncState) > 0 {
		if asyncModule == nil {
			asyncModule = &rasync.Config{}
			resource.Rule.Async = asyncModule
		}

		for i, parameter := range resource.AsyncState {
			switch strings.ToLower(parameter.Name) {
			case "userid":
				asyncModule.State.UserID = &resource.AsyncState[i].Parameter
			case "useremail":
				asyncModule.State.UserEmail = &resource.AsyncState[i].Parameter
			case "jobmatchkey":
				asyncModule.State.JobMatchKey = &resource.AsyncState[i].Parameter
			}
		}
	}
	if asyncModule == nil {
		return nil
	}

	if asyncModule.Method == "" {
		schema := url.Scheme(asyncModule.Destination, "err")
		switch schema {
		case "file", "s3", "gs":
			asyncModule.Method = async.NotificationMethodStorage
		}
	}
	if asyncModule.Jobs.Connector == nil {
		asyncModule.Jobs.Connector = view.NewRefConnector(s.DefaultConnector())
	}
	if asyncModule.State.JobMatchKey == nil {
		return fmt.Errorf("async matchKey parameter is not defined")
	}
	return nil
}
