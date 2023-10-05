package path

import (
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"time"
)

const (
	PathFileName = "paths.yaml"
)

type (
	Container struct {
		ModTime *time.Time `yaml:"ModTime,omitempty" json:",omitempty"`
		Items   []*Item    `yaml:"Items" json:"Items"`
	}

	Settings struct {
		APIKey      *APIKey      `json:",omitempty"  yaml:"APIKey,omitempty"`
		Cors        *Cors        `json:",omitempty"  yaml:"Cors,omitempty"`
		Compression *Compression `json:",omitempty"  yaml:"Compression,omitempty"`
		Redirect    *Redirect    `json:",omitempty"  yaml:"PreSign,omitempty"`
		Logger      *Logger      `json:",omitempty"  yaml:"Logger,omitempty"`
	}

	Path struct {
		contract.Path `yaml:",inline"`
		Settings      `yaml:",inline"`
		Internal      bool             `json:"Internal,omitempty" yaml:"Internal,omitempty" `
		Kind          string           `json:"Kind,omitempty" yaml:"Kind,omitempty" `
		SourceURL     string           `yaml:"-" json:"-"`
		Version       *version.Control `yaml:"-" json:"-"`
	}

	Item struct {
		SourceURL string  `yaml:"SourceURL"`
		Paths     []*Path `yaml:"Routes"`
		Settings  `yaml:",inline"`
		Version   version.Control `yaml:"Version,omitempty"`
	}
)

func (r *Settings) inherit(from *Settings) {
	if r.Cors == nil {
		r.Cors = from.Cors
		return
	}

	if r.APIKey == nil {
		r.APIKey = from.APIKey
		return
	}
	if r.Compression == nil {
		r.Compression = from.Compression
		return
	}

	if r.Logger == nil {
		r.Logger = from.Logger
		return
	}
	if r.Redirect == nil {
		r.Redirect = from.Redirect
		return
	}
}

func (p *Path) CorsEnabled() bool {
	return p.Cors != nil
}

func (p *Container) setModTime(ts time.Time) {
	p.ModTime = &ts
}
