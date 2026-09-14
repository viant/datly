package invocation

import (
	"bytes"
	"encoding/base64"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
	"testing"
)

type blobMarshalTrap struct{ *response.Buffered }

func (*blobMarshalTrap) MarshalJSON() ([]byte, error) {
	panic("transport bytes must not be marshaled as an entity")
}
func TestGenericTransportNeverInfersStructuredContent(t *testing.T) {
	for _, payload := range [][]byte{nil, {255, 0, 2}, []byte(`{"message":"not a Go entity"}`)} {
		execution := &Execution{value: &blobMarshalTrap{response.NewBuffered(response.WithBytes(payload), response.WithHeader("Content-Type", "application/custom"))}}
		result := execution.ToolResult()
		if result.StructuredContent != nil {
			t.Fatal("generic bytes inferred structured content")
		}
		blob := result.Content[0].(schema.EmbeddedResource).Resource
		actual, err := base64.StdEncoding.DecodeString(blob.Blob)
		if err != nil || !bytes.Equal(payload, actual) || *blob.MimeType != "application/custom" {
			t.Fatal(blob, err)
		}
	}
}
