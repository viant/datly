package sql

import (
	"hash/fnv"
	"strconv"
)

func StableTraceID(parts ...string) string {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}
