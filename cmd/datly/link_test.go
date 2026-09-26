package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestLinkSyncCommandIsExplicit(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
	}{
		{args: []string{"link"}, code: 2},
		{args: []string{"link", "unknown"}, code: 2},
		{args: []string{"link", "sync", "-h"}, code: 0},
		{args: []string{"link", "sync", "-dir", t.TempDir()}, code: 1},
	} {
		var out, diagnostic bytes.Buffer
		if code := run(context.Background(), test.args, &out, &diagnostic); code != test.code {
			t.Fatalf("%v: code=%d want=%d stderr=%s", test.args, code, test.code, diagnostic.String())
		}
		if len(test.args) > 1 && test.args[1] == "unknown" && !strings.Contains(diagnostic.String(), "link sync") {
			t.Fatalf("link command did not show its usage: %s", diagnostic.String())
		}
	}
}
