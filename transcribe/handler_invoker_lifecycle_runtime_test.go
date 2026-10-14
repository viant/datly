package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTranscribedGoInvokerLifecycle(t *testing.T) {
	verifyTranscribedSuccessFinalizer(t, HandlerGo, func(root, source string) string {
		source = strings.Replace(source, "func (i *EventsInput) Init(context.Context) error {", "func (i *EventsInput) Init(ctx context.Context, invoker xhandler.ComponentInvoker) error {", 1)
		source = strings.Replace(source, "func (o *EventsOutput) Finalize(context.Context) error {", "func (o *EventsOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker) error {", 1)
		source = strings.Replace(source, "_ xhandler.Initializer =", "_ xhandler.InvokerInitializer =", 1)
		source = strings.Replace(source, "_ xhandler.Finalizer   =", "_ xhandler.InvokerFinalizer   =", 1)
		source = strings.Replace(source, `"init,commit,finalize"`, `"init,finalize,commit"`, 1)
		// Composition hooks run before completion, including when preparation fails.
		// The old success-only fixture keeps its separate post-completion assertions.
		start := strings.Index(source, `	t.Run("is skipped after flush failure"`)
		end := strings.Index(source[start:], "\nvar (") + start
		source = source[:start] + "}\n" + source[end:]
		begin := strings.Index(source, "func (i *EventsInput) Init(")
		endHook := strings.Index(source[begin:], "func resetLifecycle()") + begin
		hooks := source[begin:endHook]
		source = source[:begin] + source[endHook:]
		begin = strings.Index(source, "var (")
		endVars := strings.Index(source[begin:], "\n)\n") + begin + len("\n)\n")
		vars := source[begin:endVars]
		source = source[:begin] + source[endVars:]
		hookSource := "package events\nimport (\"context\"; xhandler \"github.com/viant/xdatly/handler\")\n" + vars + hooks
		if err := os.WriteFile(filepath.Join(root, "generated", "events_invoker_hooks.go"), []byte(hookSource), 0644); err != nil {
			t.Fatal(err)
		}
		source = strings.Replace(source, "\"context\"", "\"context\"\n \"os\"", 1)
		source += `
func TestDiscoveredInvokerLifecycle(t *testing.T) {
 cwd,err:=os.Getwd();if err!=nil{t.Fatal(err)}
 sources,err:=bootstrap.DiscoverComponentsFromPackages(context.Background(),filepath.Dir(cwd),[]string{"example.com/generated/..."},nil)
 if err!=nil{t.Fatal(err)}
 if len(sources)!=1{t.Fatalf("discovered %d components",len(sources))}
 if err=sources[0].ValidateContractTypes(reflect.TypeFor[EventsInput](),reflect.TypeFor[EventsOutput]());err!=nil{t.Fatal(err)}
 if !reflect.TypeFor[*EventsInput]().Implements(reflect.TypeFor[xhandler.InvokerInitializer]()){t.Fatal("lost input lifecycle")}
 if !reflect.TypeFor[*EventsOutput]().Implements(reflect.TypeFor[xhandler.InvokerFinalizer]()){t.Fatal("lost output lifecycle")}
}
`
		return source
	})
}
