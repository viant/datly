package http

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/scy"
)

// APIKey retains original configuration names and raw URI-prefix matching.
type APIKey struct {
	URI, Header, Value string
	Secret             *scy.Resource
}
type APIKeys []APIKey

// Resolve detaches, validates and resolves secrets during generation staging.
func (keys APIKeys) Resolve(ctx context.Context) (APIKeys, error) {
	result := append(APIKeys(nil), keys...)
	seen := map[string]bool{}
	for i := range result {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := &result[i]
		if seen[key.URI] || key.URI != "" && (!strings.HasPrefix(key.URI, "/") || strings.ContainsAny(key.URI, "\r\n?#")) {
			return nil, fmt.Errorf("invalid or duplicate APIKeys URI prefix")
		}
		seen[key.URI] = true
		if key.Secret != nil {
			secret, err := scy.New().Load(ctx, key.Secret)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("APIKeys secret loading failed")
			}
			key.Value = secret.String()
			key.Secret = nil
		}
		if err := (&DocumentAccess{APIKeyHeader: key.Header, APIKeyValue: key.Value}).Validate(); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return len(result[i].URI) > len(result[j].URI) })
	return result, nil
}

// Apply acts only on a stage-owned component clone. Matching configured keys
// override authored route keys, as original gateway router assembly did.
func (keys APIKeys) Apply(component *spec.Component) {
	for _, route := range component.Routes {
		if route == nil {
			continue
		}
		if key := keys.match(route.Path); key != nil {
			route.APIKeyHeader, route.APIKeyValue = key.Header, key.Value
		}
	}
}

func (keys APIKeys) match(path string) *APIKey {
	for i := range keys {
		if strings.HasPrefix(path, keys[i].URI) {
			return &keys[i]
		}
	}
	return nil
}
