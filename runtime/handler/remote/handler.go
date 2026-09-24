// Package remote contains an ordinary remote component handler. The
// handler receives bound configuration and providers through the
// same DI path as an application-defined handler. Shared remote call planning
// lives in runtime/remote.
package remote

import (
	"context"
	"fmt"

	core "github.com/viant/datly/runtime/remote"
	xcache "github.com/viant/xdatly/cache"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
	xhandler "github.com/viant/xdatly/handler"
)

// Input is the shipped per-request input: one caller credential forwarded
// verbatim by a request mapping and the typed configuration constant.
// Component metadata may rename the incoming header; nothing here
// inspects or rewrites the credential. Applications with other request
// shapes declare their own input and call Mapper.HTTP or Mapper.MCP from Exec.
type Input struct {
	Token  string      `parameter:"Token,kind=header,in=Authorization,required"`
	Remote core.Config `parameter:"Remote,kind=const,in=Remote"`
}

// Handler is an ordinary component handler over Input and a user-declared
// output. Native Bindly resolves its static dependencies once, during runtime
// registration, before Exec can run. Register it like any other contract:
//
//	custom.New(&remote.Handler[access.Context]{})
type Handler[O any] struct {
	Mapper *core.Mapper    `bind:"kind=remote_mapper,required"`
	HTTP   xhttp.Provider  `bind:"kind=http_client"`
	MCP    xmcp.Provider   `bind:"kind=mcp_client"`
	Cache  xcache.Provider `bind:"kind=cache"`
}

// Exec selects one transport explicitly from the bound configuration.
func (h *Handler[O]) Exec(ctx context.Context, _ xhandler.Session, input *Input, output *O) error {
	if input == nil {
		return fmt.Errorf("remote input is required")
	}
	if h == nil || h.Mapper == nil {
		return fmt.Errorf("remote mapper is not bound")
	}
	switch input.Remote.Client.Transport {
	case core.TransportHTTP:
		return h.Mapper.HTTP(ctx, &input.Remote, h.HTTP, h.Cache, input, output)
	case core.TransportMCP:
		return h.Mapper.MCP(ctx, &input.Remote, h.MCP, h.Cache, input, output)
	default:
		return fmt.Errorf("remote transport %q is not supported", input.Remote.Client.Transport)
	}
}
