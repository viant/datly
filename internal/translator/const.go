package translator

import (
	"context"
	"encoding/json"
	"fmt"
)

func (r *Repository) ensureConstants(ctx context.Context) error {
	if constantResource, _ := r.loadDependency(ctx, "constants.yaml"); constantResource != nil {
		r.State.AppendViewParameters(constantResource.Parameters...)
	}
	if constantResource, _ := r.loadDependency(ctx, "variables.yaml"); constantResource != nil {
		r.State.AppendViewParameters(constantResource.Parameters...)
	}
	if URL := r.Config.repository.ConstURL; URL != "" {
		constants, err := r.loadConstantMap(ctx, URL)
		if err != nil {
			return err
		}
		r.State.AppendConstants(constants)
	}
	return nil
}

func (r *Repository) loadConstantMap(ctx context.Context, URL string) (map[string]interface{}, error) {
	data, err := r.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, fmt.Errorf("failed to load const %v %w", URL, err)
	}
	//TODO based on ext allow various format, currently only JSON
	var constMap map[string]interface{}
	if err = json.Unmarshal(data, &constMap); err != nil {
		return nil, fmt.Errorf("failed to parse const: %v %w", URL, err)
	}
	return constMap, nil
}
