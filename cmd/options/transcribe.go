package options

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/viant/afs/url"
)

// Transcribe defines options for the transcribe command which uses
// the shape pipeline exclusively (compile → plan → load) without
// depending on internal/translator.
type Transcribe struct {
	Connector
	Source     []string `short:"s" long:"src" description:"DQL source file(s)"`
	Repository string   `short:"r" long:"repo" description:"output repository location" default:"repo/dev"`
	Namespace  string   `short:"u" long:"namespace" description:"route namespace" default:"dev"`
	Module     string   `short:"m" long:"module" description:"go module location" default:"."`
	Strict     bool     `long:"strict" description:"enable strict compile mode"`
	TypeOutput string   `long:"type-output" description:"go type output directory (default: same as --module)"`
	TypeFile   string   `long:"type-file" description:"generated go file name (default: dql filename or main view in lower_underscore)"`
	Project    string   `short:"p" long:"proj" description:"project location"`
	APIPrefix  string   `short:"a" long:"api" description:"api prefix" default:"/v1/api"`
}

// DefaultConnectorName returns the first connector name from the -c flags.
func (t *Transcribe) DefaultConnectorName() string {
	if len(t.Connectors) == 0 {
		return ""
	}
	parts := strings.SplitN(t.Connectors[0], "|", 2)
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

func (t *Transcribe) Init(ctx context.Context) error {
	if t.Project == "" {
		t.Project, _ = os.Getwd()
	}
	t.Project = ensureAbsPath(t.Project)
	t.Connector.Init()
	if url.IsRelative(t.Repository) {
		t.Repository = url.Join(t.Project, t.Repository)
	}
	if url.IsRelative(t.Module) {
		t.Module = url.Join(t.Project, t.Module)
	}
	if t.TypeOutput != "" && url.IsRelative(t.TypeOutput) {
		t.TypeOutput = url.Join(t.Project, t.TypeOutput)
	}
	if len(t.Source) == 0 {
		return fmt.Errorf("transcribe: at least one --src is required")
	}
	for i := range t.Source {
		expandRelativeIfNeeded(&t.Source[i], t.Project)
	}
	if strings.TrimSpace(t.Namespace) == "" {
		t.Namespace = "dev"
	}
	return nil
}
