package version

import (
	"sync"
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
		mux        sync.RWMutex
		Version    `yaml:",inline"`
		changeKind ChangeKind
		modTime    time.Time
	}
)

func (c *Control) SetChangeKind(kind ChangeKind) {
	c.mux.Lock()
	c.changeKind = kind
	c.mux.Unlock()
}
func (c *Control) ChangeKind() ChangeKind {
	c.mux.RLock()
	ret := c.changeKind
	c.mux.RUnlock()
	return ret
}

func (c *Control) SetModTime(modTime time.Time) {
	c.mux.Lock()
	c.modTime = modTime
	c.mux.Unlock()
}

func (c *Control) ModTime() time.Time {
	c.mux.RLock()
	ret := c.modTime
	c.mux.RUnlock()
	return ret
}

func (c *Control) HasChanged(since time.Time) bool {
	return c.ModTime().Equal(since)
}

func (c *Version) Increase() {
	atomic.AddInt64(&c.SCN, 1)
}
