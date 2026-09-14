package mcpclient

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	skillformat "github.com/viant/mcp-protocol/extension/skills"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"reflect"
	"testing"
)

// SkillFile verifies one on-demand read; listing never calls it implicitly.
func SkillFile(t testing.TB, native *client.Client, skill schema.Skill, uri string) []byte {
	t.Helper()
	var file *schema.SkillResource
	for i := range skill.Resources.Files {
		if skill.Resources.Files[i].Uri == uri {
			file = &skill.Resources.Files[i]
			break
		}
	}
	if file == nil {
		t.Fatalf("file %s absent from skill inventory", uri)
	}
	read, err := native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: uri})
	if err != nil || read == nil || len(read.Contents) != 1 {
		t.Fatalf("skill read: %+v %v", read, err)
	}
	content := read.Contents[0]
	data := []byte(content.Text)
	if content.Blob != "" {
		data, err = base64.StdEncoding.DecodeString(content.Blob)
		if err != nil {
			t.Fatal(err)
		}
	}
	if int64(len(data)) != file.Size || fmt.Sprintf("sha256:%x", sha256.Sum256(data)) != file.Digest {
		t.Fatalf("skill content integrity: %s", uri)
	}
	if uri == skill.Uri {
		front, err := skillformat.Frontmatter(data)
		if err != nil || !reflect.DeepEqual(front, skill.Frontmatter) {
			t.Fatalf("frontmatter mismatch: %v", err)
		}
	}
	return data
}
