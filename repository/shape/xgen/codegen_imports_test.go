package xgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComponentCodegen_buildImports_ChecksumPathDefault(t *testing.T) {
	g := &ComponentCodegen{}
	imports := g.buildImports(false, false)
	assert.Contains(t, imports, "github.com/viant/xdatly/types/custom/dependency/checksum")
	assert.NotContains(t, imports, "github.com/viant/xdatly/types/custom/checksum")
}

func TestComponentCodegen_buildImports_ChecksumPathFromPackagePath(t *testing.T) {
	g := &ComponentCodegen{PackagePath: "github.com/acme/project/pkg/dev/vendor"}
	imports := g.buildImports(false, false)
	assert.Contains(t, imports, "github.com/acme/project/pkg/dependency/checksum")
}
