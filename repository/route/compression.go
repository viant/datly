package route

type (
	Compression struct {
		MinSizeKb int `yaml:"MinSizeKb,omitempty"`
	}

	Redirect struct {
		StorageURL   string `yaml:"StorageURL,omitempty"` ///github.com/viant/datly/v0/app/lambda/lambda/proxy.go
		MinSizeKb    int    `yaml:"MinSizeKb,omitempty"`
		TimeToLiveMs int    `yaml:"TimeToLiveMs,omitempty"`
	}
)
