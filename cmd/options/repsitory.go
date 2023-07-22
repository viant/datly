package options

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"os"
	"strings"
)

var defaultPort = 8080

type (
	Repository struct {
		Connector
		Mbus
		JwtVerifier
		ProjectURL           string
		RepositoryURL        string     `short:"r" long:"repo" description:"datly rule repository location"  default:"repo/dev" `
		ConstURL             string     `short:"O" long:"const" description:"const location" `
		Port                 *int       `short:"P" long:"port" description:"endpoint port" `
		APIPrefix            string     `short:"a" long:"api" description:"api prefix"  default:"v1/api" `
		Configs              ConfigURLs `short:"C" long:"config" description:"config url" `
		CacheConnectorPrefix string     `short:"H" long:"cprefix" description:"cache prefix"`
	}

	ConfigURL  string
	ConfigURLs []ConfigURL
)

func (c *ConfigURLs) Append(URL string) {
	*c = append(*c, ConfigURL(URL))
}

func (c ConfigURLs) URL() string {
	if len(c) == 0 {
		return ""
	}
	return string(c[0])
}

func (c ConfigURLs) URLs() []string {
	var result = make([]string, 0, len(c))
	for _, URL := range c {
		result = append(result, string(URL))
	}
	return result
}

func (c ConfigURLs) Repository() string {
	for _, config := range c {
		if repo := config.Repository(); repo != "" {
			return repo
		}
	}
	return ""
}
func (c ConfigURL) Repository() string {
	if index := strings.LastIndex(string(c), "Datly/config.json"); index != -1 {
		return string(c[:index])
	}
	return ""
}
func (r *Repository) Init(ctx context.Context, project string) error {
	r.ProjectURL = project
	if project == "" {
		r.ProjectURL, _ = os.Getwd()
	}
	configRepo := r.Configs.Repository()
	if r.RepositoryURL == "" && configRepo != "" {
		r.RepositoryURL = configRepo
	}
	if r.RepositoryURL == "" {
		return fmt.Errorf("rule repository location was empty")
	}
	if r.APIPrefix == "" {
		r.APIPrefix = "/v1/api"
	}
	r.Connector.Init()
	r.JwtVerifier.Init()
	expandRelativeIfNeeded(&r.RepositoryURL, project)
	expandRelativeIfNeeded(&r.ConstURL, project)
	if configRepo == "" {
		r.Configs.Append(url.Join(r.RepositoryURL, "Datly/config.json"))
	}

	err := r.validateConfigURLs()
	if err != nil {
		return err
	}
	return nil
}

// validateConfigURLs validates if config URL that is no part of repository URL have actual file
func (r *Repository) validateConfigURLs() error {
	if len(r.Configs) > 0 {
		var qualified ConfigURLs
		for _, URL := range r.Configs {
			ok, _ := fs.Exists(context.Background(), string(URL))
			if !ok {
				if URL.Repository() == "" {
					return fmt.Errorf("invalid config URL: %v", URL)
				}
				continue
			}
			qualified.Append(string(URL))
		}
		r.Configs = qualified
	}
	return nil
}
