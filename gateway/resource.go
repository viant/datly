package gateway

import (
	"github.com/viant/cloudless/resource"
	"path"
)

type (
	ResourcesChange struct {
		pluginsIndex   *ExtIndex
		resourcesIndex *ExtIndex
		changed        bool
	}

	ExtIndex struct {
		deletedIndex map[string]bool
		deleted      []string
		updatedIndex map[string]bool
		updated      []string
	}
)

func NewResourcesChange() *ResourcesChange {
	return &ResourcesChange{
		resourcesIndex: NewExtIndex(),
		pluginsIndex:   NewExtIndex(),
	}
}

func NewExtIndex() *ExtIndex {
	return &ExtIndex{
		deletedIndex: map[string]bool{},
		updatedIndex: map[string]bool{},
	}
}

func (c *ResourcesChange) OnChange(operation resource.Operation, URL string) {
	ext := path.Ext(URL)
	index, ok := c.ExtIndex(ext)
	if !ok {
		return
	}

	c.changed = true

	switch operation {
	case resource.Added, resource.Modified:
		index.ResourceUpdated(URL)
	case resource.Deleted:
		index.ResourceDeleted(URL)
	}
}

func (c *ResourcesChange) ExtIndex(ext string) (*ExtIndex, bool) {
	switch ext {
	case ".yaml":
		return c.resourcesIndex, true
	case ".so":
		return c.pluginsIndex, true
	}

	return nil, false
}

func (c *ResourcesChange) Changed() bool {
	return c.changed
}

func (i *ExtIndex) ResourceUpdated(url string) {
	if i.updatedIndex[url] {
		return
	}

	i.updated = append(i.updated, url)
	i.updatedIndex[url] = true
}

func (i *ExtIndex) ResourceDeleted(url string) {
	if i.deletedIndex[url] {
		return
	}

	i.deleted = append(i.deleted, url)
	i.deletedIndex[url] = true
}

func (i *ExtIndex) Changed(URL string) bool {
	return i.updatedIndex[URL] || i.deletedIndex[URL]
}
