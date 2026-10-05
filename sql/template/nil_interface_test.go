package template

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	xlogger "github.com/viant/xdatly/logger"
)

func TestCompilerNilInterfaceInput(t *testing.T) {
	type input struct {
		Logger xlogger.Logger
		ID     int
	}
	program, err := (Compiler{Source: `#if($ID > 0) SELECT $ID #end`, InputType: reflect.TypeOf(input{})}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name string
		log  xlogger.Logger
	}{{"absent", nil}, {"present", log}, {"typed nil", (*slog.Logger)(nil)}, {"absent again", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Logger: tc.log, ID: 7})})
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(result.SQL) != "SELECT :ID" || len(result.Args) != 0 {
				t.Fatalf("unexpected SQL result: %+v", result)
			}
		})
	}
}

func TestCompilerNilInterfaceBinder(t *testing.T) {
	type input struct{ ID int }
	variable := Variable{Name: "Logger", Type: reflect.TypeOf((*xlogger.Logger)(nil)).Elem(), Key: "logger"}
	compile := func(required bool) Evaluator {
		variable.Required = required
		p, err := (Compiler{Source: `#if($ID > 0) SELECT $ID #end`, InputType: reflect.TypeOf(input{}), Variables: []Variable{variable}}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	program := compile(false)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name   string
		binder templateBinder
	}{{"absent", nil}, {"present", templateBinder{"logger": log}}, {"typed nil", templateBinder{"logger": (*slog.Logger)(nil)}}, {"explicit nil", templateBinder{"logger": nil}}, {"absent again", templateBinder{}}} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{ID: 7}), Binder: tc.binder})
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(result.SQL) != "SELECT :ID" || len(result.Args) != 0 {
				t.Fatalf("unexpected SQL result: %+v", result)
			}
		})
	}
	if _, err := compile(true).Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{ID: 7})}); err == nil || !strings.Contains(err.Error(), "binder is required") {
		t.Fatalf("required absent binder error changed: %v", err)
	}
	if _, err := compile(true).Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{ID: 7}), Binder: templateBinder{}}); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("required binding error changed: %v", err)
	}
}
