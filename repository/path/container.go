package path

import (
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/utils/httputils"
	"net/http"
	"strings"
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
		APIKey       *APIKey      `json:",omitempty"  yaml:"APIKey,omitempty"`
		Cors         *Cors        `json:",omitempty"  yaml:"Cors,omitempty"`
		Compression  *Compression `json:",omitempty"  yaml:"Compression,omitempty"`
		Redirect     *Redirect    `json:",omitempty"  yaml:"PreSign,omitempty"`
		Logger       *Logger      `json:",omitempty"  yaml:"Logger,omitempty"`
		RevealMetric *bool
		With         []string `yaml:"With" json:"With"`
	}

	Handler struct {
		MessageBus string `json:"MessageBus,omitempty" yaml:"MessageBus,omitempty" `
		With       []string
	}

	Path struct {
		contract.Path `yaml:",inline"`
		Settings      `yaml:",inline"`
		Handler       *Handler         `yaml:"Handler" json:"Handler"`
		Internal      bool             `json:"Internal,omitempty" yaml:"Internal,omitempty" `
		Connector     string           `json:",omitempty"`
		ContentURL    string           `json:"ContentURL,omitempty" yaml:"ContentURL,omitempty" `
		SourceURL     string           `yaml:"-" json:"-"`
		Version       *version.Control `yaml:"-" json:"-"`
	}

	Item struct {
		SourceURL  string  `yaml:"SourceURL"`
		MessageBus string  `json:"MessageBus,omitempty" yaml:"MessageBus,omitempty" `
		Paths      []*Path `yaml:"Routes" json:"Routes"`
		Settings   `yaml:",inline"`
		Version    version.Control `yaml:"-" json:"-"`
	}
)

func (r *Settings) HasWith(candidate string) bool {
	if len(r.With) == 0 {
		return false
	}
	for _, item := range r.With {
		if item == candidate {
			return true
		}
	}
	return false
}

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
	if r.RevealMetric == nil {
		r.RevealMetric = from.RevealMetric
		return
	}
	if len(r.With) == 0 {
		r.With = from.With
		return
	}
}

func (p *Path) IsMetricsEnabled(req *http.Request) bool {
	return p.IsMetricInfo(req) || p.IsMetricDebug(req)
}

func (p *Path) IsMetricInfo(req *http.Request) bool {
	if !p.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyInfoHeaderValue
}

func (p *Path) IsMetricDebug(req *http.Request) bool {
	if !p.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyDebugHeaderValue
}

func (p *Path) CorsEnabled() bool {
	return p.Cors != nil
}

func (p *Path) IsRevealMetric() bool {
	if p.RevealMetric == nil {
		return false
	}
	return *p.RevealMetric
}

func (p *Container) setModTime(ts time.Time) {
	p.ModTime = &ts
}
