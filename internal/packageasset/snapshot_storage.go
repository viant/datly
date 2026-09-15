package packageasset

import (
	"archive/zip"
	"io"
	"io/fs"
)

// snapshotFS retains the immutable archive and opens stored file sections with
// independent seek positions, as required by HTTP range serving. Directories
// retain the standard archive filesystem behavior.
type snapshotFS struct {
	*zip.Reader
	storage io.ReaderAt
	files   map[string]snapshotEntry
}

type snapshotEntry struct {
	info   fs.FileInfo
	offset int64
}

func (s *snapshotFS) Open(name string) (fs.File, error) {
	entry, ok := s.files[name]
	if !ok {
		return s.Reader.Open(name)
	}
	return &snapshotFile{SectionReader: io.NewSectionReader(s.storage, entry.offset, entry.info.Size()), info: entry.info}, nil
}

type snapshotFile struct {
	*io.SectionReader
	info fs.FileInfo
}

func (f *snapshotFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *snapshotFile) Close() error               { return nil }
