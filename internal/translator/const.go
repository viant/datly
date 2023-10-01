package translator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"strings"
)

func (r *Repository) ensureConstants(ctx context.Context) error {
	if constantResource, _ := r.loadDependency(ctx, "constants.yaml"); constantResource != nil {
		r.State.AppendViewParameters(constantResource.Parameters...)
	}
	if constantResource, _ := r.loadDependency(ctx, "variables.yaml"); constantResource != nil {
		r.State.AppendViewParameters(constantResource.Parameters...)
	}
	if URL := r.Config.repository.ConstURL; URL != "" {
		constants, err := r.loadMap(ctx, URL)
		if err != nil {
			return err
		}
		r.State.AppendConst(constants)
	}
	return nil
}

func (r *Repository) ensureSubstitutes(ctx context.Context) error {
	if substitutesResource, _ := r.loadDependency(ctx, "substitutes.yaml"); substitutesResource != nil {
		r.Substitutes = substitutesResource.Substitutes
	}
	if URL := r.Config.repository.SubstitutesURL; URL != "" {
		aMap, err := r.loadMap(ctx, URL)
		if err != nil {
			return err
		}
		for k, v := range aMap {
			fragment := toolbox.AsString(v)
			if !strings.HasPrefix(k, "$") {
				k = "$" + k
			}
			r.Substitutes = append(r.Substitutes, &view.Substitute{Key: k, Fragment: fragment})
		}
	}
	return nil
}

func (r *Repository) loadMap(ctx context.Context, URL string) (map[string]interface{}, error) {
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
