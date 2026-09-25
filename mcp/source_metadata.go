package mcp

// ToolSourceMetaKey is the MCP tool metadata entry identifying the exact
// published Datly component that backs a tool. Hosts supply this entry from
// their authoritative publication catalog; ordinary Datly components do not
// synthesize a source version.
const ToolSourceMetaKey = "viant.datly/source"

// ToolSourceIdentity is the host-attested component identity carried under
// ToolSourceMetaKey. A consumer must still evaluate its own current ACL and
// require an exact match with the configured resource before calling the tool.
type ToolSourceIdentity struct {
	Tenant   string `json:"tenant"`
	ReportID string `json:"reportId"`
	Version  int    `json:"version"`
}
