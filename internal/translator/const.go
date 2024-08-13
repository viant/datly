package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"path"
	"strings"
)

func (r *Repository) ensureConstants(ctx context.Context) error {
	if constantResource, _ := r.loadDependency(ctx, "constants.yaml"); constantResource != nil {
		r.State.AppendViewParameters(constantResource.Parameters...)
	}
	return nil
}

func (r *Repository) ensureSubstitutes(ctx context.Context) error {
	r.Substitutes = map[string]view.Substitutes{}
	for _, URL := range r.Config.repository.SubstitutesURL {
		_, name := url.Split(URL, file.Scheme)

		if index := strings.LastIndex(name, "."); index != -1 {
			name = name[:index]
		}

		if profile := r.Config.repository.Profile; profile != "" {
			name = strings.Replace(name, "_"+profile, "", 1)
			name = strings.Replace(name, profile, "", 1)
		}
		aMap, err := r.loadMap(ctx, URL)
		if err != nil {
			return err
		}
		r.Substitutes[name] = map[string]string{}
		for k, v := range aMap {
			fragment := toolbox.AsString(v)
			r.Substitutes[name][k] = fragment
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
