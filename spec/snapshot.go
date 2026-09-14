package spec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type Snapshot struct {
	Version     string       `json:"version"`
	Keys        []Key        `json:"keys,omitempty"`
	Types       []TypeRef    `json:"types,omitempty"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
	Meta        []Provenance `json:"provenance,omitempty"`
}

func MarshalSnapshot(snapshot Snapshot) ([]byte, error) {
	return json.Marshal(snapshot)
}

func HashSnapshot(snapshot Snapshot) (string, error) {
	data, err := MarshalSnapshot(snapshot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
