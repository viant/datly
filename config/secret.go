package config


//Secret represents a secret config
type Secret struct {
	URL       string `json:",omitempty"`
	Parameter string `json:",omitempty"`
	Key       string `json:",omitempty"`
}

