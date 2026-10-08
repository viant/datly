package tool

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/viant/datly/exec"
	"github.com/viant/mcp-protocol/schema"
)

// ComponentBindingMetaKey is both the tools/list observed identity entry and
// the tools/call params._meta expected identity entry. Business arguments do
// not carry component identity and cannot select a current-generation alias.
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

func expectedComponentBinding(request *schema.CallToolRequest) (*exec.ComponentBinding, error) {
	raw, err := json.Marshal(request.Params.Meta)
	if err != nil {
		return nil, exec.ErrComponentBinding
	}
	var metadata map[string]json.RawMessage
	if json.Unmarshal(raw, &metadata) != nil {
		return nil, exec.ErrComponentBinding
	}
	raw, found := metadata[ComponentBindingMetaKey]
	if !found {
		return nil, nil
	}
	var binding exec.ComponentBinding
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&binding) != nil || binding.Validate() != nil {
		return nil, exec.ErrComponentBinding
	}
	return &binding, nil
}
