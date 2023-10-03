package route

type Audit struct {
	URL     string              `yaml:"URL,omitempty"`
	Headers map[string][]string `yaml:"Headers,omitempty"`
}
