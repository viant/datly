package compiler

import xcodec "github.com/viant/xdatly/codec"

// Codec exposes the exact constructed codec to registration's metadata compiler.
// The same instance is used by Transform; names do not establish its authority.
func (t *codecTransformer) Codec() xcodec.Instance { return t.codec }
