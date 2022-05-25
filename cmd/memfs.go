package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

func fsAddJSON(fs afs.Service, URL string, any interface{}) error {
	data, err := json.MarshalIndent(any, "", "\t")
	if err != nil {
		return err
	}
	return fs.Upload(context.Background(), URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func fsAddYAML(fs afs.Service, URL string, any interface{}) error {
	aMap := map[string]interface{}{}
	data, _ := json.Marshal(any)
	json.Unmarshal(data, &aMap)
	compacted := map[string]interface{}{}
	toolbox.CopyNonEmptyMapEntries(aMap, compacted)
	data, err := yaml.Marshal(compacted)
	if err != nil {
		return err
	}
	return fs.Upload(context.Background(), URL, file.DefaultFileOsMode, bytes.NewReader(data))
}
