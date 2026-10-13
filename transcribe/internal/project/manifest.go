package project

import (
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

const ManifestVersion = 1

type Manifest struct {
	Version      int             `json:"version"`
	Components   []Component     `json:"components"`
	Dependencies []string        `json:"dependencies,omitempty"`
	Analysis     MigrationReport `json:"analysis"`
}

type Component struct {
	Key          spec.Key               `json:"key"`
	Source       string                 `json:"source,omitempty"`
	Package      string                 `json:"package"`
	OutputType   string                 `json:"outputType,omitempty"`
	OutputOrigin typecatalog.TypeOrigin `json:"outputOrigin,omitempty"`
	IR           string                 `json:"ir"`
	Diagnostics  string                 `json:"diagnostics"`
	Artifacts    []string               `json:"artifacts"`
	Routes       []Route                `json:"routes,omitempty"`
	Dependencies []string               `json:"dependencies,omitempty"`
}

type Route struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type MigrationReport struct {
	Components []MigrationComponent `json:"components"`
}

type MigrationComponent struct {
	Key             spec.Key `json:"key"`
	InputOwnership  string   `json:"inputOwnership"`
	OutputOwnership string   `json:"outputOwnership"`
	GeneratedViews  int      `json:"generatedViews"`
	LinkedViews     int      `json:"linkedViews"`
	Handler         string   `json:"handler"`
}

func (route Route) Identity() string {
	method := strings.ToUpper(strings.TrimSpace(route.Method))
	path := strings.TrimSpace(route.Path)
	if method == "" || path == "" {
		return ""
	}
	return method + " " + path
}
