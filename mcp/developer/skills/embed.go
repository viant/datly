// Package skills contains the complete canonical authoring skill snapshot.
// Regenerate/check with go run ./internal/cmd/skillpack from the module root.
package skills

import (
	"embed"
	"github.com/viant/datly/mcp/resource"
)

//go:embed all:assets
var files embed.FS

func Folders() []resource.Folder {
	var result []resource.Folder
	for _, name := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		result = append(result, resource.Folder{Namespace: name, Root: "assets/" + name, URIPrefix: "skill://" + name + "/", FS: files, Skills: []string{"."}})
	}
	return result
}
