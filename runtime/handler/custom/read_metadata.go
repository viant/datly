package custom

import (
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

func (h *contractHandler[I, O]) RequiresReadMetadata() bool {
	if h == nil || (xshape.Runtime{}).IsNil(h.contract) {
		return false
	}
	consumer, ok := h.contract.(xhandler.ReadMetadataConsumer)
	return ok && consumer.RequiresReadMetadata()
}
