package codegen

import (
	"fmt"
	"strings"
)

type customTypeRegistry struct {
	Elements []string
	index    map[string]bool
}

func (c *customTypeRegistry) register(dataType string) {
	c.ensureIndex()
	if _, ok := c.index[dataType]; ok {
		return
	}
	c.index[dataType] = true
	c.Elements = append(c.Elements, fmt.Sprintf(registerTypeTemplate, dataType, dataType))
}

func (c *customTypeRegistry) stringify() string {
	return strings.Join(c.Elements, "\n")
}

func (c *customTypeRegistry) ensureIndex() {
	if len(c.index) == 0 {
		c.index = map[string]bool{}
	}
}
