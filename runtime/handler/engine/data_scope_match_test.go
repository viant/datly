package engine

import (
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

type matchedDMLProbe struct {
	table string
	match *xhandler.Match
	verb  string
}

func (*matchedDMLProbe) Insert(string, any) error     { return nil }
func (*matchedDMLProbe) Execute(string, ...any) error { return nil }
func (*matchedDMLProbe) Update(string, any) error     { return nil }
func (*matchedDMLProbe) Delete(string, any) error     { return nil }
func (p *matchedDMLProbe) UpdateWithOptions(table string, _ any, options ...xhandler.Option) error {
	settings := &xhandler.Options{}
	for _, apply := range options {
		apply(settings)
	}
	p.table, p.match, p.verb = table, settings.IfMatch, "update"
	return nil
}
func (p *matchedDMLProbe) DeleteWithOptions(table string, _ any, options ...xhandler.Option) error {
	settings := &xhandler.Options{}
	for _, apply := range options {
		apply(settings)
	}
	p.table, p.match, p.verb = table, settings.IfMatch, "delete"
	return nil
}

func TestFocusedDMLCapabilityForwardsMatch(t *testing.T) {
	probe := &matchedDMLProbe{}
	capability := dmlCapability{service: probe}
	for _, verb := range []string{"update", "delete"} {
		var err error
		if verb == "update" {
			err = capability.UpdateWithOptions("records", struct{}{}, xhandler.WithIfMatch("etag", 7))
		} else {
			err = capability.DeleteWithOptions("records", struct{}{}, xhandler.WithIfMatch("etag", 7))
		}
		if err != nil || probe.verb != verb || probe.table != "records" || probe.match == nil || probe.match.Column != "etag" || probe.match.Value != 7 {
			t.Fatalf("%s match not forwarded: probe=%+v err=%v", verb, probe, err)
		}
	}
}
