package plugins

import "time"

const PluginConfig = "Config"
const PackageName = "PackageName"
const TypesName = "Types"

type Metadata struct {
	CreationTime time.Time `json:",omitempty"`
	URL          string    `json:",omitempty"`
	Version      string    `json:",omitempty"`
}
