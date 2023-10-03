package version

import (
	"sync/atomic"
	"time"
)

const (
	ChangeKindModified = "modified"
	ChangeKindDeleted  = "deleted"
)

type (
	Version struct {
		SCN int64 `yaml:"SCN,omitempty"` //sequence change number
	}

	//ChangeKind defines change types
	ChangeKind string
	Control    struct {
		Version    `yaml:",inline"`
		ChangeKind ChangeKind `yaml:"-"`
		ModTime    time.Time  `yaml:"ModTime,inline"`
	}
)

func (c *Version) Increase() {
	atomic.AddInt64(&c.SCN, 1)
}
