package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestTranscribeCommandArguments(t *testing.T) {
	for _, args := range [][]string{
		{"gen", "-op", "patch", "example.com/app/orders"}, {"transcribe"}, {"transcribe", "delete", "example.com/app/orders"},
		{"transcribe", "-op", "patch", "example.com/app/orders"},
		{"transcribe", "patch", "-op", "get", "example.com/app/orders"},
		{"transcribe", "patch", "-lang", "java", "example.com/app/orders"},
		{"transcribe", "get"}, {"transcribe", "put"}, {"transcribe", "post"},
	} {
		var out, diagnostic bytes.Buffer
		if code := run(context.Background(), args, &out, &diagnostic); code != 2 {
			t.Fatalf("%v: exit %d, %s", args, code, diagnostic.String())
		}
	}
	for _, op := range []string{"", "get", "patch", "post", "put", "handler"} {
		args := []string{"transcribe"}
		if op != "" {
			args = append(args, op)
		}
		args = append(args, "-h")
		var out, diagnostic bytes.Buffer
		if code := run(context.Background(), args, &out, &diagnostic); code != 0 || !strings.Contains(diagnostic.String(), "datly transcribe get|patch|post|put") || strings.Contains(diagnostic.String(), "-op ") {
			t.Fatalf("%v: exit %d, %s", args, code, diagnostic.String())
		}
	}
}
