// Package config loads standalone configuration without opening listeners or databases.
package config

import (
	"fmt"
	"github.com/viant/datly/constant"
	"net"
	"strconv"
	"time"

	"github.com/viant/datly/bootstrap/connector"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/scy/auth/jwt/verifier"
)

type Config struct {
	// ConstURL identifies one trusted flat YAML/JSON instance constant mapping.
	ConstURL string
	Const    *constant.Values `json:"-"`
	gateway.Config
	URL         string `json:"-"`
	Version     string
	Endpoint    Endpoint
	Info        *openapi3.Info
	GoBootstrap *Packages
	Warmup      *Warmup
	Observation *Observation
	// BaseDir and ModuleDirs locate trusted local source modules. Package selection
	// retains original GoBootstrap names; discovery is owned by transcribe.
	BaseDir       string
	ModuleDirs    []string
	Connector     string
	Connectors    []connector.Config
	JWTValidator  *verifier.Config
	MCP           *MCP
	RouteURL      string
	PluginsURL    string
	DependencyURL string
	Jobs          *Jobs
	JobURL        string
	FailedJobURL  string
	MaxJobs       int
}

type Packages struct{ Packages, Exclude []string }

type Endpoint struct {
	Port int
	// Address is an explicit listener override, including port zero for OS allocation.
	Address             string
	ReadTimeoutMs       int
	ReadHeaderTimeoutMs int
	IdleTimeoutMs       int
	WriteTimeoutMs      int
	MaxHeaderBytes      int
	ShutdownTimeoutMs   int
}

type MCP struct {
	Folders       []resource.Folder
	Port          *int
	Address       string
	Authorization *authorization.Policy
}

func (e Endpoint) ListenAddress() (string, error) {
	if e.Port < 0 || e.Port > 65535 || e.ShutdownTimeoutMs < 0 {
		return "", fmt.Errorf("invalid endpoint port, timeout or header limit")
	}
	const maxMs = int64((1<<63 - 1) / int64(time.Millisecond))
	for _, value := range []int{e.ReadHeaderTimeoutMs, e.IdleTimeoutMs, e.ReadTimeoutMs, e.WriteTimeoutMs, e.ShutdownTimeoutMs} {
		if int64(value) > maxMs || int64(value) < -maxMs {
			return "", fmt.Errorf("endpoint timeout overflows duration")
		}
	}
	if e.Address != "" {
		if e.Port != 0 {
			return "", fmt.Errorf("Endpoint.Address and Port are mutually exclusive")
		}
		_, port, err := net.SplitHostPort(e.Address)
		number, numberErr := strconv.Atoi(port)
		if err != nil || numberErr != nil || number < 0 || number > 65535 {
			return "", fmt.Errorf("invalid endpoint address")
		}
		return e.Address, nil
	}
	port := e.Port
	if port == 0 {
		port = 8080
	}
	return net.JoinHostPort("", strconv.Itoa(port)), nil
}

func (e Endpoint) ShutdownTimeout() time.Duration {
	if e.ShutdownTimeoutMs == 0 {
		return 5 * time.Second
	}
	return time.Duration(e.ShutdownTimeoutMs) * time.Millisecond
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("standalone configuration is required")
	}
	if (c.GoBootstrap == nil || len(c.GoBootstrap.Packages) == 0) && len(c.StaticContent) == 0 {
		return fmt.Errorf("GoBootstrap.Packages is required")
	}
	if _, err := c.Endpoint.ListenAddress(); err != nil {
		return err
	}
	// These locations are normalized by the loader, but have no deployment owner
	// in this slice. Reject them rather than starting with partially applied policy.
	if c.RouteURL != "" || c.PluginsURL != "" {
		return fmt.Errorf("RouteURL and PluginsURL deployment is not supported by this standalone slice")
	}
	if err := c.validateJobs(); err != nil {
		return err
	}
	if c.MCP != nil {
		if _, err := c.MCP.ListenAddress(); err != nil {
			return fmt.Errorf("MCP: %w", err)
		}
	}
	return c.validateServices()
}

func (m MCP) ListenAddress() (string, error) {
	if m.Port != nil {
		if m.Address != "" || *m.Port < 0 || *m.Port > 65535 {
			return "", fmt.Errorf("invalid or conflicting MCP port")
		}
		return net.JoinHostPort("", strconv.Itoa(*m.Port)), nil
	}
	if m.Address == "" {
		return "", fmt.Errorf("MCP requires Port or Address")
	}
	return (Endpoint{Address: m.Address}).ListenAddress()
}
