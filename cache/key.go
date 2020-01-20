package cache

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"hash/fnv"
	"path"
	"strings"
)

//GetKey returns key
func GetKey(view string, sql *dsc.ParametrizedSQL) string {
	name := getHash(sql) +".cache"
	return path.Join(view, name)
}

func getHash(sql *dsc.ParametrizedSQL) string {
	expr := sql.SQL
	if len(sql.Values) > 0 {
		for i := range sql.Values {
			expr = strings.Replace(expr, "?", toolbox.AsString(sql.Values[i]), 1)
		}
	}
	return fmt.Sprintf("%v_%v", fnvHash(expr), md5Hash(expr))
}


//Hash returns fnv fnvHash value
func md5Hash(key string) string {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	data :=  h.Sum(nil)
	return base64.StdEncoding.EncodeToString(data)
}

//Hash returns fnv fnvHash value
func fnvHash(key string) int {
	h := fnv.New64()
	_, _ = h.Write([]byte(key))
	data := h.Sum(nil)
	keyNumeric := int64(0)
	shift := 0
	for i := 0; i < 8 && i < len(data); i++ {
		v := int64(data[len(data)-1-i])
		if shift == 0 {
			keyNumeric |= v
		} else {
			keyNumeric |= v << uint64(shift)
		}
		shift += 8
	}
	if keyNumeric < 0 {
		keyNumeric *= -1
	}
	return int(keyNumeric)
}

