package options

import (
	"fmt"
	"github.com/viant/afs/url"
)

type CacheWarmup struct {
	URIs []string `short:"u" long:"wuri" description:"uri to warmup cache" `
	Run
}

type CacheProvider struct {
	Location     string `short:"l" long:"cloc" description:"cache location" default:"${view.Name}" `
	Name         string `short:"n" long:"cname" description:"cache name" default:"aero" `
	ProviderURL  string `short:"u" long:"curl" description:"cache provider url" `
	TimeToLiveMs int    `short:"t" long:"cttl"  description:"cache time to live ms" default:"3600000"`
}

func (c CacheProvider) Init() error {
	if c.ProviderURL == "" || c.Name == "" {
		return nil
	}
	schema := url.Scheme(c.ProviderURL, "")
	switch schema {
	case "aerospike":
	default:
		return fmt.Errorf("unsupported cache provider: %v", schema)
	}
	return nil
}
