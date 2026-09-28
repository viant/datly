// Package manifestfree holds the shared CLI/API generation-to-SQLite fixture.
package manifestfree

import _ "embed"

//go:embed runtime.go.txt
var RuntimeSource string
