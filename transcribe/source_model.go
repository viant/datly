package transcribe

import (
	"os"
	"path/filepath"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	xdocs "github.com/viant/xdatly/docs"
)

// Source is the transcribe-owned authored source/config model used to assemble
// canonical components and generated package artifacts for package bootstrap.
type Source struct {
	Scope     string
	Name      string
	Path      string
	Text      string
	Connector string
	// Resources is the Bindly-owned resource authority shared by source
	// expansion, column discovery, and generated-package planning.
	Resources *resource.Store
	Docs      xdocs.Service
	Types     *typecatalog.Catalog
	// ColumnRefiner explicitly enables compile-time SQLX column discovery.
	// A nil refiner keeps ordinary transcription DB-free.
	ColumnRefiner *column.Refiner
	// PackageComponent is optional package-authority canonical metadata used as
	// the base for DQL+package transcription. Authored DQL overlays this graph;
	// ordinary package-only bootstrap does not enter transcribe.
	PackageComponent *spec.Component
	// GoHandler is an explicitly accepted custom handler source asset. It is
	// generation input only; canonical metadata retains just its handler name.
	GoHandler *gen.GoHandlerAsset
	// VeltyHandler optionally supplies an accepted template asset. When absent,
	// explicit DQL service programs are transcribed into the same artifact.
	// Runtime state and capabilities remain owned by runtime/handler/velty.
	VeltyHandler *gen.VeltyHandlerAsset
}

func (s *Source) BaseDir() string {
	if s == nil {
		return ""
	}
	location := filepath.Clean(s.Path)
	if location == "" || location == "." {
		return ""
	}
	if info, err := os.Stat(location); err == nil {
		if info.IsDir() {
			return location
		}
		return filepath.Dir(location)
	}
	if filepath.Ext(location) != "" {
		return filepath.Dir(location)
	}
	return location
}
