package repository

import (
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
)

func (r *Service) updateCacheConnectorRef(aResource *view.Resource, aView *view.View) error {
	prefix := r.options.cacheConnectorPrefix
	if prefix == "" {
		return nil
	}
	cacheWarmup := aView.Warmup()
	if cacheWarmup != nil {
		if cacheWarmup.Connector != nil && cacheWarmup.Connector.Ref != "" {
			cacheConnectorName := prefix + cacheWarmup.Connector.Ref
			if aResource.ExistsConnector(cacheConnectorName) {
				cacheWarmup.Connector.Ref = cacheConnectorName
			}
		} else if cacheWarmup.Connector == nil {
			viewConnector, ok := r.viewConnector(aResource, aView)
			if ok {
				refName := prefix + viewConnector.Name
				if ok && aResource.ExistsConnector(refName) {
					cacheWarmup.Connector = &view.Connector{Reference: shared.Reference{Ref: refName}}
				}
			}
		}
	}
	for _, relation := range aView.With {
		if err := r.updateCacheConnectorRef(aResource, &relation.Of.View); err != nil {
			return err
		}
	}
	return nil
}

func (r *Service) viewConnector(aResource *view.Resource, aView *view.View) (*view.Connector, bool) {
	if aView.Connector.Name != "" {
		return aView.Connector, true
	}
	if aView.Connector.Ref != "" {
		connector, err := aResource.Connector(aView.Connector.Ref)
		return connector, err == nil
	}
	return nil, false
}
