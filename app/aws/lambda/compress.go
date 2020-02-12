package lambda

import (
	"encoding/base64"
	"github.com/viant/afs/option/content"
	"github.com/viant/datly/app/aws/apigw"
	"github.com/viant/datly/shared"
	"strings"
)

func compressIfNeeded(response *apigw.ProxyResponse) {
	if len(response.Body) > CompressionLimit {
		if compressed, err := shared.Compress(strings.NewReader(response.Body)); err == nil {
			response.RawBody = compressed.Bytes()
			response.Body = base64.StdEncoding.EncodeToString(response.RawBody)
			compressed := true
			response.Compressed = &compressed
			response.IsBase64Encoded = true
			response.Headers[content.Encoding] = shared.EncodingGzip
			response.Headers[content.Type] = shared.ContentTypeJSON
		}
	}
}
