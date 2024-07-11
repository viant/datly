package translator

import (
	"context"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/view"
)

func (r *Repository) persistMBus(cfg *Config) error {
	if len(r.MessageBuses) == 0 {
		return nil
	}
	resource := view.Resource{MessageBuses: r.MessageBuses}
	connectors, err := asset.EncodeYAML(resource)
	if err != nil {
		return err
	}
	r.Files.Append(asset.NewFile(url.Join(cfg.DependencyURL, "mbus.yaml"), string(connectors)))
	return nil
}

func (r *Repository) ensureMBus(ctx context.Context) error {
	var messageBuses = map[string]*mbus.Resource{}
	var mbusResource []*mbus.Resource
	if mbusOptions := r.Config.repository.Mbus; len(mbusOptions.MBuses) > 0 {
		for _, item := range mbusOptions.MBuses {
			resource, err := mbus.EncodedResource(item).Decode()
			if err != nil {
				return err
			}
			messageBuses[resource.Name] = resource
			mbusResource = append(mbusResource, resource)
		}
	}

	//load previous defined message buses
	if prevResource, _ := r.loadDependency(ctx, "mbus.yaml"); prevResource != nil {
		for _, item := range prevResource.MessageBuses {
			if _, ok := messageBuses[item.Name]; ok {
				continue
			}
			mbusResource = append(mbusResource, item)
		}
	}
	r.MessageBuses = mbusResource
	return nil
}
