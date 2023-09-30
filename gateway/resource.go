package gateway

import (
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/view"
	"path"
	"strings"
)

type (
	RouterChanges struct {
		routers        map[string]*router.Router
		pluginsIndex   *ExtIndex //.pinf
		resourcesIndex *ExtIndex //.yml, .yaml
		routersIndex   *ExtIndex //.rt
		sqlIndex       *ExtIndex // .sql
		metaIndex      *ExtIndex // */.meta/...
		changed        bool
	}

	ExtIndex struct {
		deleted *FilesIndex
		updated *FilesIndex
	}

	FilesIndex struct {
		data     []string
		index    map[string]int
		resolved map[string]bool
	}
)

func (i *FilesIndex) RemoveEntry(key string) {
	keyIndex, ok := i.index[key]
	if !ok {
		return
	}

	delete(i.index, key)
	if len(i.data) > 1 {
		lastKey := i.data[len(i.data)-1]
		i.index[lastKey] = keyIndex
		delete(i.index, lastKey)
		i.data[keyIndex] = lastKey
	}

	i.data = i.data[:len(i.data)-1]
}

func (i *FilesIndex) MarkResolved(URL string) {
	i.resolved[URL] = true
}

func (i *FilesIndex) Add(url string) {
	if _, ok := i.index[url]; ok {
		return
	}

	i.index[url] = len(i.data)
	i.data = append(i.data, url)
}

func (i *FilesIndex) Contains(URL string) bool {
	_, ok := i.index[URL]
	return ok
}

func NewResourcesChange(routers map[string]*router.Router) *RouterChanges {
	routersCopy := map[string]*router.Router{}

	for key, value := range routers {
		routersCopy[key] = value
	}

	return &RouterChanges{
		routers:        routersCopy,
		pluginsIndex:   NewExtIndex(),
		resourcesIndex: NewExtIndex(),
		routersIndex:   NewExtIndex(),
		sqlIndex:       NewExtIndex(),
		metaIndex:      NewExtIndex(),
	}
}

func NewExtIndex() *ExtIndex {
	return &ExtIndex{
		deleted: NewFilesIndex(),
		updated: NewFilesIndex(),
	}
}

func NewFilesIndex() *FilesIndex {
	return &FilesIndex{
		data:  nil,
		index: map[string]int{},
	}
}

func (c *RouterChanges) OnChange(operation resource.Operation, URL string) {
	index, ok := c.ExtIndex(URL)
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

func (c *RouterChanges) ExtIndex(URL string) (*ExtIndex, bool) {
	ext := path.Ext(URL)
	if strings.Contains(URL, ".meta/") && (ext == ".yaml" || ext == ".yml") {
		return c.metaIndex, true
	}

	switch ext {
	case ".yaml", ".yml":
		return c.resourcesIndex, true
	case ".sql":
		return c.sqlIndex, true
	case ".pinf": //go plugin info ext
		return c.pluginsIndex, true
	case ".rt":
		return c.routersIndex, true
	}
	return nil, false
}

func (c *RouterChanges) Changed() bool {
	return c.changed
}

func (c *RouterChanges) AfterResourceChanges() {
	for routerURL, aRouter := range c.routers { // detect routers where view SQL changes
		if c.routerSQLChanged(aRouter, c.sqlIndex.updated.data) {
			c.routersIndex.Changed(routerURL)
		}
	}

	for _, metaDeleted := range c.metaIndex.deleted.data { // detect routers where file with columns meta was deleted
		routerURL := strings.Replace(metaDeleted, ".meta/", "", 1)
		if !c.routersIndex.deleted.Contains(routerURL) {
			c.routersIndex.ResourceUpdated(routerURL)
		}
	}

	for _, routerURL := range c.routersIndex.deleted.data { // delete routers where rule file was deleted
		delete(c.routers, routerURL)
	}

	c.metaIndex = NewExtIndex()
	c.sqlIndex = NewExtIndex()
	c.routersIndex.deleted = NewFilesIndex()
}

func (c *RouterChanges) routerSQLChanged(aRouter *router.Router, sqls []string) bool {
	if len(sqls) == 0 {
		return false
	}

	routes := aRouter.Routes("")
	for _, route := range routes {
		if c.viewSQLChanged(route.View, sqls) {
			return true
		}
	}

	return false
}

func (c *RouterChanges) viewSQLChanged(aView *view.View, sqlFiles []string) bool {
	if len(sqlFiles) == 0 {
		return false
	}

	if aView.Template.SourceURL != "" {
		for _, sqlFile := range sqlFiles {
			if strings.HasSuffix(sqlFile, aView.Template.SourceURL) {
				return true
			}
		}
	}

	for _, relation := range aView.With {
		if c.viewSQLChanged(&relation.Of.View, sqlFiles) {
			return true
		}
	}

	return false
}

func (c *RouterChanges) AddRouter(url string, aRouter *router.Router) {
	c.routers[url] = aRouter
	c.routersIndex.updated.RemoveEntry(url)
}

func (i *ExtIndex) ResourceUpdated(url string) {
	i.updated.Add(url)
}

func (i *ExtIndex) ResourceDeleted(url string) {
	i.deleted.Add(url)
}

func (i *ExtIndex) Changed(URL string) bool {
	return i.deleted.Contains(URL) || i.updated.Contains(URL)
}
