package resource

import (
	"encoding/base64"
	"mime"
	"strings"
)

func isTextMIME(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/") || mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

func encodeBlob(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}
