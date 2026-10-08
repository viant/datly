package spec

import (
	"fmt"
	"go/ast"
	"go/token"
	"io/fs"
	"strings"
)

// BorrowedSQLRow is explicit, generation-only authoring provenance. It neither
// schedules the owner nor grants the borrower mutation or presence authority.
type BorrowedSQLRow struct {
	BodyPath      string `json:"bodyPath"`
	Package       string `json:"package"`
	Type          string `json:"type"`
	OwnerURI      string `json:"ownerURI"`
	OwnerName     string `json:"ownerName"`
	OwnerBodyPath string `json:"ownerBodyPath"`
	SourceStart   int    `json:"-"`
	SourceEnd     int    `json:"-"`
}

func (r BorrowedSQLRow) Validate() error {
	for _, path := range []string{r.BodyPath, r.OwnerBodyPath} {
		for _, holder := range strings.Split(path, "/") {
			if !token.IsIdentifier(holder) || !ast.IsExported(holder) {
				return fmt.Errorf("borrow_sql_row body path %q requires exact exported holder names", path)
			}
		}
	}
	if !token.IsIdentifier(r.Type) || !ast.IsExported(r.Type) || !token.IsIdentifier(r.OwnerName) || r.OwnerName == "_" {
		return fmt.Errorf("borrow_sql_row requires an exported row name and exact owner Source.Name")
	}
	if !fs.ValidPath(r.OwnerURI) || !strings.HasSuffix(r.OwnerURI, ".dql") || strings.Contains(r.OwnerURI, "\\") {
		return fmt.Errorf("borrow_sql_row requires a confined relative owner DQL resource URI")
	}
	if !fs.ValidPath(r.Package) || !strings.Contains(strings.Split(r.Package, "/")[0], ".") || strings.ContainsAny(r.Package, "\\:") {
		return fmt.Errorf("borrow_sql_row requires a canonical package import path")
	}
	return nil
}
