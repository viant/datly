package readerbuilder

import (
	"context"
	"strings"
	"testing"
)

func TestServiceCreatesInitialReaderGraph(t *testing.T) {
	response := New(Config{Name: "reader", AvailableConnectors: []string{"main"}}).Apply(context.Background(), Request{Operation: Operation{
		Type: OperationCreateReader,
		Reader: &ReaderMutation{
			Package: "example.com/app/dynamic/vendors", Connector: "main", Route: "/vendors", Name: "vendor_catalog",
			SQL: "SELECT ID, NAME FROM VENDOR",
		},
	}})
	if !response.Applied || len(response.Diagnostics) != 0 || response.Structure == nil || response.Structure.Component == nil {
		t.Fatalf("response=%+v", response)
	}
	for _, expected := range []string{
		"#package('example.com/app/dynamic/vendors')\n\n#setting",
		"$VendorCatalogs<[]*VendorCatalog>(output/view)",
		"type(vendor_catalog, 'VendorCatalog')",
		"FROM (SELECT ID, NAME FROM VENDOR) vendor_catalog",
	} {
		if !strings.Contains(response.DQL, expected) {
			t.Fatalf("missing %q in:\n%s", expected, response.DQL)
		}
	}
	if response.Structure.Component.RootView == nil || response.Structure.Component.RootView.Namespace != "vendor_catalog" {
		t.Fatalf("root=%+v", response.Structure.Component.RootView)
	}
}

func TestServiceCreateReaderRejectsOverwriteAndUnknownConnector(t *testing.T) {
	service := New(Config{Name: "reader", AvailableConnectors: []string{"main"}})
	mutation := &ReaderMutation{Package: "example.com/app/vendors", Connector: "missing", Route: "/vendors", Name: "vendors", SQL: "SELECT ID FROM VENDOR"}
	for _, source := range []string{"", "SELECT existing.* FROM EXISTING existing"} {
		response := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationCreateReader, Reader: mutation}})
		if response.Applied || response.DQL != source || len(response.Diagnostics) == 0 {
			t.Fatalf("source=%q response=%+v", source, response)
		}
	}
}
