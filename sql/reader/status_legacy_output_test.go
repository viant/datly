package reader

import (
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
	"reflect"
	"testing"
)

func TestLegacyStatusOutputExactAccessorAndPreservation(t *testing.T) {
	type alias = legacy.Status
	type named struct {
		State legacy.Status
		Data  []int
	}
	type embedded struct {
		legacy.Status
		Data []int
	}
	type pointer struct {
		State *legacy.Status
		Data  []int
	}
	type embeddedPointer struct {
		*legacy.Status
		Data []int
	}
	type aliasHolder struct{ State alias }
	var typednil *int
	warning := []*legacy.Warning{{Message: "original", Reason: "reason"}}
	extras := map[string]interface{}{"false": false, "zero": 0, "empty": "", "nil": nil, "typedNil": typednil}
	for _, value := range []interface{}{nil, false, 0, "", typednil, map[string]interface{}{"RawKey": false}} {
		before := legacy.Status{Status: "prior", Message: "retained", Errors: value, Warning: warning, Extras: extras}
		for _, tc := range []struct {
			name, field string
			out         any
			status      func() *legacy.Status
		}{
			{"named", "State", &named{State: before, Data: []int{7}}, nil},
			{"embedded", "Status", &embedded{Status: before, Data: []int{7}}, nil},
			{"pointer", "State", &pointer{State: &before, Data: []int{7}}, nil},
			{"embedded-pointer", "Status", &embeddedPointer{Status: &before, Data: []int{7}}, nil},
			{"alias", "State", &aliasHolder{State: before}, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				a := &outputAccessors{}
				if err := a.compileStatus(tc.field, reflect.TypeOf(tc.out)); err != nil {
					t.Fatal(err)
				}
				if err := a.writeSuccess(tc.out); err != nil {
					t.Fatal(err)
				}
				field := reflect.ValueOf(tc.out).Elem().FieldByName(tc.field)
				if field.Kind() == reflect.Ptr {
					field = field.Elem()
				}
				got := field.Interface().(legacy.Status)
				if got.Status != "ok" || got.Message != before.Message || !reflect.DeepEqual(got.Errors, value) || reflect.ValueOf(got.Warning).Pointer() != reflect.ValueOf(warning).Pointer() || reflect.ValueOf(got.Extras).Pointer() != reflect.ValueOf(extras).Pointer() {
					t.Fatalf("unrelated original metadata changed: %+v", got)
				}
				data := reflect.ValueOf(tc.out).Elem().FieldByName("Data")
				if data.IsValid() && !reflect.DeepEqual(data.Interface(), []int{7}) {
					t.Fatal("neighbor Data changed")
				}
			})
		}
	}
	for _, tc := range []struct {
		field string
		out   any
		want  any
	}{{"State", &pointer{}, &pointer{State: &legacy.Status{Status: "ok"}}}, {"Status", &embeddedPointer{}, &embeddedPointer{Status: &legacy.Status{Status: "ok"}}}} {
		a := &outputAccessors{}
		if err := a.compileStatus(tc.field, reflect.TypeOf(tc.out)); err != nil {
			t.Fatal(err)
		}
		if err := a.writeSuccess(tc.out); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tc.out, tc.want) {
			t.Fatalf("original nil allocation changed: %#v", tc.out)
		}
	}
}

func TestLegacyStatusOutputDoesNotAdmitLookalikes(t *testing.T) {
	type defined legacy.Status
	type lookalike struct {
		Status  string
		Message string
		Errors  interface{}
		Warning []*legacy.Warning
		Extras  map[string]interface{}
	}
	for _, out := range []any{&struct{ State defined }{}, &struct{ State lookalike }{}, &struct{ State **legacy.Status }{}, &struct{ State int }{}, &struct{ Other string }{}} {
		a := &outputAccessors{}
		if err := a.compileStatus("State", reflect.TypeOf(out)); err == nil {
			t.Fatalf("unapproved status type accepted: %T", out)
		}
	}
	type modernHolder struct{ State modern.Status }
	out := &modernHolder{State: modern.Status{Status: "prior", Message: "retained", Error: 42}}
	a := &outputAccessors{}
	if err := a.compileStatus("State", reflect.TypeOf(out)); err != nil {
		t.Fatal(err)
	}
	if err := a.writeSuccess(out); err != nil {
		t.Fatal(err)
	}
	if out.State != (modern.Status{"ok", "retained", 42}) {
		t.Fatal("modern exact holder changed")
	}
}
