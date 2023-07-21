package config

type CodecConfig struct {
	Query       string `json:",omitempty"`
	SourceURL   string `json:",omitempty"`
	Source      string `json:",omitempty"`
	OutputType  string `json:",omitempty"`
	HandlerType string `json:",omitempty"`
}
