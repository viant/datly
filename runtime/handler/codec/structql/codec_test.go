package structql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	xcodec "github.com/viant/xdatly/codec"
)

func TestFactoryAggregatesIDsIntoNamedDestination(t *testing.T) {
	type event struct{ Id int }
	type helper struct{ Values []int }
	factory := Factory{}
	codec, err := factory.New(&xcodec.Config{
		Body: Name, SourceType: reflect.TypeOf([]event{}), DestinationType: reflect.TypeOf(helper{}),
		Args: []string{"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := codec.Value(context.Background(), []event{{Id: 7}, {Id: 11}})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := actual.(helper)
	if !ok || !reflect.DeepEqual(result.Values, []int{7, 11}) {
		t.Fatalf("StructQL result = %#v", actual)
	}
}

func TestFactoryPreservesPointerDestination(t *testing.T) {
	type event struct{ Id int }
	type helper struct{ Values []int }
	codec, err := (Factory{}).New(&xcodec.Config{
		Body: Name, SourceType: reflect.TypeOf([]event{}), DestinationType: reflect.TypeOf((*helper)(nil)),
		Args: []string{"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := codec.Value(context.Background(), []event{{Id: 7}, {Id: 11}})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := actual.(*helper)
	if !ok || !reflect.DeepEqual(result.Values, []int{7, 11}) {
		t.Fatalf("StructQL pointer result = %#v", actual)
	}
}

func TestFactoryRejectsInterfaceBearingContracts(t *testing.T) {
	type event struct{ ID int }
	type unsafeSource struct{ ID any }
	type unsafeDestination struct{ Values []any }
	tests := []struct {
		name        string
		source      reflect.Type
		destination reflect.Type
		wantPath    string
	}{
		{name: "source", source: reflect.TypeOf([]unsafeSource{}), destination: reflect.TypeOf(struct{ Values []int }{}), wantPath: "source[].ID"},
		{name: "destination", source: reflect.TypeOf([]event{}), destination: reflect.TypeOf(unsafeDestination{}), wantPath: "destination.Values[]"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := (Factory{}).New(&xcodec.Config{
				Body: Name, SourceType: testCase.source, DestinationType: testCase.destination,
				Args: []string{"SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1"},
			})
			if err == nil || !strings.Contains(err.Error(), testCase.wantPath) {
				t.Fatalf("Factory.New() error = %v, want path %q", err, testCase.wantPath)
			}
		})
	}
}

func TestCompiledCodecSupportsConcurrentInvocations(t *testing.T) {
	type event struct{ Id int }
	type helper struct{ Values []int }
	codec, err := (Factory{}).New(&xcodec.Config{
		Body: Name, SourceType: reflect.TypeOf([]event{}), DestinationType: reflect.TypeOf(helper{}),
		Args: []string{"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	const invocationCount = 32
	errs := make(chan error, invocationCount)
	var wait sync.WaitGroup
	for index := 0; index < invocationCount; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			actual, valueErr := codec.Value(context.Background(), []event{{Id: index}, {Id: index + 1}})
			if valueErr != nil {
				errs <- valueErr
				return
			}
			result, ok := actual.(helper)
			if !ok || !reflect.DeepEqual(result.Values, []int{index, index + 1}) {
				errs <- fmt.Errorf("invocation %d result = %#v", index, actual)
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
