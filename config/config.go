package config

type CodecConfig struct {
	Query     string `json:",omitempty"`
	SourceURL string `json:",omitempty"`
	Source    string `json:",omitempty"`
	JSONType  string `json:",omitempty"`
}
