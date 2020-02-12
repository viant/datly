package lambda

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/app/aws/apigw"
	"github.com/viant/datly/shared"
	"strings"
)

func storeResponse(ctx context.Context, baseURL string, jobID string, response *apigw.ProxyResponse) (*option.PreSign, error) {
	fs := afs.New()
	URL := url.Join(baseURL, ResponseFolder, jobID) + ".json"
	preSign := option.NewPreSign(PreSignTimeToLive)
	kv := []string{content.Type, shared.ContentTypeJSON}
	if response.IsCompressed() {
		response.Body = string(response.RawBody)
		response.RawBody = nil
		kv = append(kv, content.Encoding, shared.EncodingGzip)

	}
	meta := content.NewMeta(kv...)
	err := fs.Upload(ctx, URL, 0666, strings.NewReader(response.Body), preSign, meta)
	return preSign, err
}
