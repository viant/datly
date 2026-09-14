package observability

import (
	"github.com/viant/xdatly/response"
	"io"
	"os"
	"testing"
)

func TestCaptureWithoutLoggerDoesNotWriteStdout(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previous; reader.Close(); writer.Close() }()
	recorder := &Recorder{}
	recorder.Read("", &response.Metric{View: "records", Rows: 1})
	recorder.SQL("", "records", &response.SQLExecution{Error: "private error", CacheStats: &response.CacheStats{FoundLazy: true}})
	writer.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("unconfigured capture wrote stdout: %s", data)
	}
}
