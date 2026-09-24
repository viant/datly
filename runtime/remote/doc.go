// Package remote supplies request/response mapping and an opt-in cache for
// ordinary typed custom handlers that invoke an HTTP endpoint or MCP tool.
//
// The mapper plans explicit request and response mappings and invokes a
// borrowed HTTP or MCP client. A typed const configuration selects the
// protocol. When caching is enabled, a separately bound xdatly/cache.Provider
// resolves an explicitly named backend; the mapper owns no cached values.
// Input paths use compiled transform selectors, response paths use RFC 6901
// pointers, and Bindly assigns typed output fields. The mapper constructs or
// closes no transport clients or cache backends.
package remote
