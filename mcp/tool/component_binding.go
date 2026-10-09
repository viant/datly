package tool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/viant/datly/exec"
	"github.com/viant/mcp-protocol/schema"
)

// ComponentBindingMetaKey identifies the rejected legacy wire extension.
// Component identity belongs to resource/datasource definitions, not MCP meta.
const ComponentBindingMetaKey = "viant.datly/component"

// LinkedComponentBinding derives only the route contract fingerprint. Artifact
// revision and content fingerprint must already be attested by the deployment.
func LinkedComponentBinding(plan *Plan, artifact exec.LinkedArtifact) (exec.ComponentBinding, error) {
	if plan == nil || artifact.Validate() != nil {
		return exec.ComponentBinding{}, exec.ErrComponentBinding
	}
	metadata := plan.Metadata()
	raw, err := json.Marshal(struct {
		Target exec.ComponentTarget `json:"target"`
		Input  interface{}          `json:"input"`
		Output interface{}          `json:"output"`
	}{plan.Target(), metadata.InputSchema, metadata.OutputSchema})
	if err != nil {
		return exec.ComponentBinding{}, err
	}
	hash := sha256.Sum256(raw)
	binding := exec.ComponentBinding{Kind: "linked", ID: plan.Target().Component.String(), Revision: artifact.Revision,
		ContentFingerprint: artifact.ContentFingerprint, SchemaFingerprint: hex.EncodeToString(hash[:])}
	return binding, binding.Validate()
}

func rejectLegacyComponentBinding(request *schema.CallToolRequest) error {
	raw, err := json.Marshal(request.Params.Meta)
	if err != nil {
		return err
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return err
	}
	if _, found := metadata[ComponentBindingMetaKey]; found {
		return fmt.Errorf("MCP component wire metadata is unsupported; declare component identity on the resource or datasource")
	}
	return nil
}
