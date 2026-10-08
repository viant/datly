package exec

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var ErrComponentBinding = errors.New("exact component binding unavailable or mismatched")
var componentFingerprint = regexp.MustCompile(`^[a-f0-9]{64}$`)

// LinkedArtifact is trusted deployment provenance for the executable and all
// its linked resources. Revision is an explicit immutable artifact revision;
// neither an application build label nor a generation counter supplies it.
type LinkedArtifact struct {
	Revision           string `json:"revision"`
	ContentFingerprint string `json:"contentFingerprint"`
}

func (a LinkedArtifact) Validate() error {
	if !validArtifactRevision(a.Revision) || !componentFingerprint.MatchString(a.ContentFingerprint) {
		return ErrComponentBinding
	}
	return nil
}

// ComponentBinding identifies the exact selected source and public route
// contract. It carries no authorization and has no dependency on a host UI.
type ComponentBinding struct {
	Kind               string `json:"kind"`
	ID                 string `json:"id"`
	Revision           string `json:"revision"`
	ContentFingerprint string `json:"contentFingerprint"`
	SchemaFingerprint  string `json:"schemaFingerprint"`
}

func (b ComponentBinding) Validate() error {
	if strings.TrimSpace(b.ID) != b.ID || b.ID == "" || !validArtifactRevision(b.Revision) ||
		!componentFingerprint.MatchString(b.ContentFingerprint) || !componentFingerprint.MatchString(b.SchemaFingerprint) {
		return ErrComponentBinding
	}
	switch b.Kind {
	case "linked":
	case "dynamic":
		version, err := strconv.ParseUint(b.Revision, 10, 64)
		if err != nil || version == 0 || strconv.FormatUint(version, 10) != b.Revision {
			return ErrComponentBinding
		}
	default:
		return ErrComponentBinding
	}
	return nil
}

func ValidateComponentBinding(expected, actual ComponentBinding) error {
	if expected.Validate() != nil || actual.Validate() != nil || expected != actual {
		return ErrComponentBinding
	}
	return nil
}

func validArtifactRevision(revision string) bool {
	if revision == "" || strings.TrimSpace(revision) != revision || strings.ContainsAny(revision, "\x00\r\n\t") {
		return false
	}
	switch strings.ToLower(revision) {
	case "active", "latest", "working", "default":
		return false
	}
	return true
}
