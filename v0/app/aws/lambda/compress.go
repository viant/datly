package lambda

import (
	"encoding/base64"
	"github.com/viant/afs/option/content"
	"github.com/viant/datly/v0/app/aws/apigw"
	shared2 "github.com/viant/datly/v0/shared"
	"strings"
)

func compressIfNeeded(response *apigw.ProxyResponse) {
	if len(response.Body) > CompressionLimit {
		if compressed, err := shared2.Compress(strings.NewReader(response.Body)); err == nil {
			response.RawBody = compressed.Bytes()
			response.Body = base64.StdEncoding.EncodeToString(response.RawBody)
			compressed := true
			response.Compressed = &compressed
			response.IsBase64Encoded = true
			response.Headers[content.Encoding] = shared2.EncodingGzip
			response.Headers[content.Type] = shared2.ContentTypeJSON
		}
	}
}
