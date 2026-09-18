package dql

import (
	"strings"
	"testing"
)

func TestSetPackagePreservesSource(t *testing.T) {
	source := "#package('example.com/old/reader')\n\n#import('example.com/model')\n\nSELECT 1\n"
	updated, err := SetPackage(source, "example.com/users/alice/reader", "example.com/old/reader")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(updated, "#package(\"example.com/users/alice/reader\")\n\n#import") || !strings.HasSuffix(updated, "\n\nSELECT 1\n") {
		t.Fatalf("updated source=%q", updated)
	}
	if _, err = SetPackage(source, "example.com/new", "example.com/other"); err == nil {
		t.Fatal("expected package mismatch")
	}
	if _, err = SetPackage(source, "../bad", ""); err == nil {
		t.Fatal("expected invalid package path")
	}
}
