package dependency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"go/build"
	"io/fs"
	"path"
	"strings"

	"github.com/viant/datly/internal/packageasset"
	loaderast "github.com/viant/x/loader/ast"
	"github.com/viant/x/syntetic/model"
)

// Session retains parsed dependency syntax for one sequential transcription.
// Catalogs and resolved dependency closures remain private to each rule.
// It is not safe for concurrent use.
type Session struct {
	// DisableReuse measures the ordinary loader without cache or cloning overhead.
	DisableReuse bool
	packages     map[string]packageEntry
	Parses       int
	Hits         int
}
type packageEntry struct {
	digest [32]byte
	pkg    *model.Package
}
type sessionKey struct{}

func WithSession(ctx context.Context, session *Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, session)
}

func loadPackageFS(ctx context.Context, source fs.FS, dir, identity string) (*model.Package, error) {
	session, _ := ctx.Value(sessionKey{}).(*Session)
	if session != nil && session.DisableReuse {
		session.Parses++
		return loaderast.LoadPackageFS(ctx, source, dir)
	}
	if session == nil {
		return loaderast.LoadPackageFS(ctx, source, dir)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Content hashes detect equal-size edits, removals and additions, including
	// regeneration within the same filesystem timestamp resolution.
	entries, err := fs.ReadDir(source, dir)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	var files []packageasset.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		data, err := fs.ReadFile(source, path.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if bytes.Contains(data, []byte("//go:embed")) {
			session.Parses++
			return loaderast.LoadPackageFS(ctx, source, dir)
		}
		files = append(files, packageasset.File{Path: path.Join(dir, entry.Name()), Data: data})
		fmt.Fprintf(hash, "%d:%s:%d:", len(entry.Name()), entry.Name(), len(data))
		hash.Write(data)
	}
	if len(files) == 0 {
		session.Parses++
		delete(session.packages, identity)
		return nil, &build.NoGoError{Dir: dir}
	}
	data, err := fs.ReadFile(source, "go.mod")
	if err != nil {
		return nil, err
	}
	hash.Write(data)
	files = append(files, packageasset.File{Path: "go.mod", Data: data})
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	entry, ok := session.packages[identity]
	if !ok || entry.digest != digest {
		frozen, err := (packageasset.Snapshotter{Overlay: files}).Files(ctx, nil)
		if err != nil {
			return nil, err
		}
		pkg, err := loaderast.LoadPackageFS(ctx, namedSource{FS: frozen, names: source}, dir)
		session.Parses++
		if err != nil {
			delete(session.packages, identity)
			return nil, err
		}
		entry = packageEntry{digest: digest, pkg: pkg}
		if session.packages == nil {
			session.packages = map[string]packageEntry{}
		}
		session.packages[identity] = entry
	} else {
		session.Hits++
	}
	// Linking attaches runtime identity to Types and authority loading attaches
	// Dependencies. Neither mutation may alter the cached source authority.
	// Syntax remains read-only here: Catalog.RegisterPackage performs the deep
	// clone before any compiler/resolver can mutate it. Cloning that AST again at
	// this boundary costs more than reparsing it.
	result := *entry.pkg
	result.Dependencies = nil
	result.Types = make([]*model.Type, len(entry.pkg.Types))
	for i, typ := range entry.pkg.Types {
		if typ != nil {
			copy := *typ
			result.Types[i] = &copy
		}
	}
	byType := map[*model.Type]*model.Type{}
	for i, typ := range entry.pkg.Types {
		byType[typ] = result.Types[i]
	}
	result.Files = make([]*model.GoFile, len(entry.pkg.Files))
	for i, file := range entry.pkg.Files {
		copy := *file
		copy.Types = make([]*model.Type, len(file.Types))
		for j, typ := range file.Types {
			copy.Types[j] = byType[typ]
		}
		result.Files[i] = &copy
	}
	return &result, nil
}

// Retain build-selected import package names while parsing exactly the bytes
// whose fingerprint is cached.
type namedSource struct {
	fs.FS
	names fs.FS
}

func (s namedSource) PackageName(importPath string) string {
	if source, ok := s.names.(interface{ PackageName(string) string }); ok {
		return source.PackageName(importPath)
	}
	return ""
}
