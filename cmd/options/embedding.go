package options

import (
	"encoding/json"
	"errors"
	"github.com/viant/toolbox/data"
	"strings"
)

type embedding struct {
	fragment  string
	asset     string
	variables data.Map
}

// newEmbedding creates a new embedding
func newEmbedding(text string) (*embedding, error) {
	variables := data.NewMap()
	var result = &embedding{variables: variables}
	// Find the first occurrence of "${embed:"
	index := strings.Index(text, "${embed")
	if index == -1 {
		return nil, nil
	}

	fragment := text[index:]
	result.fragment = fragment

	var asset string
	var endIndex int

	// Check if the fragment starts with JSON map format
	if strings.HasPrefix(fragment, "${embed(") {
		endIndex = strings.Index(fragment, ")")
		if endIndex == -1 {
			return result, errors.New("invalid format: missing closing '):'")
		}

		jsonPart := fragment[len("${embed("):endIndex]

		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(jsonPart), &jsonMap); err != nil {
			return result, err
		}
		for k, v := range jsonMap {
			variables[k] = v
		}

		assetPart := fragment[len(jsonPart)+len("${embed(")+1:]
		assetBeginIndex := strings.Index(assetPart, ":")
		if assetBeginIndex == -1 {
			return result, errors.New("invalid format: missing ':' after JSON map")
		}
		assetPart = assetPart[assetBeginIndex+1:]
		endIndexAsset := strings.Index(assetPart, "}")
		if endIndexAsset == -1 {
			return result, errors.New("invalid format: missing closing '}'")
		}
		// Extract the asset part after "):"
		asset = assetPart[:endIndexAsset]
		asset = strings.TrimSpace(asset)
		result.asset = asset
		endIndex = strings.Index(fragment, asset)
		result.fragment = fragment[:endIndex+len(asset)+1]
		return result, nil
	}
	// Original format processing
	endIndex = strings.Index(fragment, "}")
	if endIndex != -1 {
		fragment = fragment[:endIndex+1]
		result.fragment = fragment
	}
	asset = fragment[len("${embed:"):endIndex]
	asset = strings.TrimSpace(asset)
	result.asset = asset
	return result, nil

}
