package xgen

import (
	"reflect"
	"testing"
)

func TestParseType_MapStringInterface(t *testing.T) {
	got := parseType("map[string]interface{}")
	want := reflect.TypeOf(map[string]interface{}{})
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestParseType_MapStringAny(t *testing.T) {
	got := parseType("map[string]any")
	want := reflect.TypeOf(map[string]interface{}{})
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestParseType_NestedMapSlice(t *testing.T) {
	got := parseType("[]map[string]interface{}")
	want := reflect.TypeOf([]map[string]interface{}{})
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
