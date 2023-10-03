package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"path"
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
		if len(r.Substitutes) == 0 {
			r.Substitutes = map[string]string{}
		}
		for k, v := range aMap {
			fragment := toolbox.AsString(v)
			r.Substitutes[k] = fragment
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
	replaced := r.Substitutes.Replace(string(data))
	data = []byte(replaced)

	ext := path.Ext(url.Path(URL))
	var constMap map[string]interface{}
	if err = shared.UnmarshalWithExt(data, &constMap, ext); err != nil {
		return nil, fmt.Errorf("failed to parse const: %v %w", URL, err)
	}
	return constMap, nil
}
