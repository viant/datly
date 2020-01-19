package config

import (
	"datly/base"
	"encoding/json"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v2"
)



func loadTarget(data []byte, ext string, provider func() interface{}, onLoaded func(target interface{}) error) error {
	target := provider()
	switch ext {
	case base.YAMLExt:
		ruleMap := map[string]interface{}{}
		err := yaml.Unmarshal(data, &ruleMap)
		if err != nil {
			return err
		}
		if err := toolbox.DefaultConverter.AssignConverted(target, ruleMap);err != nil {
			return err
		}
	default:
		if err := json.Unmarshal(data, target); err != nil {
			return err
		}
	}
	return onLoaded(target)
}
