# Handler-Only DQL Transcription

Use `Generator{Operation: "handler"}` to generate a registration for an existing
native Go handler. `get` remains reader generation; `post`, `put`, and `patch`
remain mutation generation. The authored HTTP method does not select execution
semantics.

## Source-Authored Factory

New registrations can declare the native factory directly, without compiling
that factory or its contracts into the transcription executable:

```sql
/* {
  "URI": "/convert", "Method": "POST", "Name": "Convert",
  "Factory": "example.com/app/conversion.NewConvert",
  "InputType": "example.com/app/conversion.Input",
  "OutputType": "example.com/app/conversion.Output",
  "MCPTool": true
} */
#package('example.com/app/generated/convert')
#setting($_ = $case_format('lc'))
```

Use `transcribe handler`, or the same discovery/generator API below without
`HandlerBindings`. `Factory` identifies a handler declaration without `Type`.
`#package` is mandatory destination authority; constructors and connectors are
never inferred. Qualified symbols may use explicit DQL import aliases.

Datly uses Go's build-selected export data to validate the actual function and
contract identities. No reflection fallback, application-wide registry, or
constructor execution is needed. Contracts resolve to exported nongeneric named
structs; genuine aliases to those structs are accepted and emitted using their
underlying named identity. DQL parameter declarations must agree with the source
contract's bindings, types, and optionality. Remove obsolete declarations after
auditing their behavior rather than silently dropping them.

`Source.GoBuild` / `Discovery.GoBuild` optionally supplies a `gobuild.Context`
(`github.com/viant/datly/transcribe/gobuild`) with `Dir`, `Tags`, and environment
overrides. Otherwise the source directory and inherited Go environment apply.
Use the same build selection as the application's workspace and deployed binary.
The Go toolchain and source dependencies must be available. Module/workspace
replacements, platform and CGO selection remain in effect.
Automatic and explicit vendoring are preserved; module updates are disabled.

Before publication, Datly builds the staged registration at its final package
location through a Go file overlay. This includes retained handwritten files and
planned removals, checks cycles and internal-package visibility, and works before
the destination exists. Preview and commit both validate; no `go run`, test
execution, package initialization, or database discovery occurs. Build failures
leave the generated files and ownership manifest unchanged.
Metadata reload uses the same build-selected source files as validation.
Source-authored factories require persistent ownership (`merge` or `overwrite`),
not `EphemeralOwnership`'s detached-module publication path.

Existing compiled mappings remain supported. If both forms select a legacy
`Type`, factory, destination, and actual contract identities must agree.

## Legacy Declaration

The SQL-free legacy JSON header can remain unchanged:

```sql
/* {
  "URI": "/convert", "Method": "POST", "Name": "Convert",
  "Type": "legacy/conversion.Handler",
  "InputType": "legacy/conversion.Input",
  "OutputType": "legacy/conversion.Output",
  "Description": "Convert an expression", "MCPTool": true
} */
```

## Explicit Compiled Mapping

The application's compiled transcription command supplies the legacy-name mapping,
the destination, and the actual factory. No constructor names are guessed.

```go
binding, err := transcribe.NewHandlerBinding[conversion.Input, conversion.Output](
    transcribe.HandlerMapping{
        LegacyType:         "legacy/conversion.Handler",
        LegacyInput:        "legacy/conversion.Input",
        LegacyOutput:       "legacy/conversion.Output",
        FactoryPackage:     "example.com/app/conversion",
        FactoryName:        "NewConvert",
        DestinationPackage: "example.com/app/generated/convert",
    }, conversion.NewConvert,
)
// Handle err before discovery or publication.

project, err := (&transcribe.Discovery{
    BaseDir: root,
    Include: []string{"example.com/app/dql/conversion"},
    HandlerBindings: []*transcribe.HandlerBinding{binding},
}).Compile(ctx)
// Check err and require exactly one component before indexing Components.

generated, err := (transcribe.Generator{
    Operation: "handler",
    GenerationPolicy: generate.GenerationPolicyOverwrite,
}).Generate(ctx, transcribe.GenerationRequest{
    Compiled: project.Components[0], Destination: root,
})
```

The factory must be a top-level exported function with the exact signature
`func() handler.Contract[I, O]` from `github.com/viant/xdatly/handler`. Go type
aliases are accepted; distinct named types and unrelated lookalike interfaces
are not. The factory is inspected, never called, during discovery, generation,
or regeneration. Contract types must be exported named structs. The destination
must be separate from the handwritten factory and contract packages.

## Generated Linkage

The holder refers directly to the imported input and output contracts, preserving
methods, initialization, and custom JSON encoding. Its `DatlyHandler` capability
uses the existing native handler adapter. Include that generated holder/package
in the application's normal eager/indexed bootstrap selection. Datly constructs
the handler through its existing runtime lifecycle.

`links.go` also exposes `RegisterConvertFactories(registry)` for applications that
assemble explicit exports. Call it from the application's registry assembly if
using that registration path; merely generating the file does not register it.
Retire only the old component holder to avoid duplicate routes, not the factory,
contracts, or handwritten business logic.

No SQL, reader view, mutation, or business-handler implementation is generated.
Normal manifest ownership, merge/overwrite policies, and staged publication apply.

## Declaration And Connector Policy

Choose handler intent in the compiled CLI **before** selecting/opening discovery
connectors. No `ColumnRefiner` is needed; handler compilation never calls one even
if supplied. Datly cannot undo a connection that the calling wrapper opened first.
The CLI syntax is `transcribe handler`. A legacy `Type` still requires a compiled
mapping; an explicit source-backed `Factory` does not.

`Source.Connector` / `Discovery.Connector` and the header's `Connector` supply
explicit runtime metadata only. Nothing defaults to `ci_ads` or any other
connector. Conflicting explicit connector names fail. Database schema flags do
not apply to handler transcription.

Legacy parameter declarations must agree with the linked input contract. They
do not overwrite its fields, binding tags, defaults, or codecs. Unknown parameters
fail. An obsolete, explicitly optional input may be acknowledged through
`HandlerMapping.UnusedParameters`, keyed by its exact logical name with a nonempty
reason. Datly emits a `DQL-HANDLER-UNUSED` warning and does not create a new field.
This option cannot hide a parameter already present in the linked contract.
Required declarations cannot be discarded this way.

SQL, conflicting modern settings, unsupported header fields, missing mappings,
wrong factory identities, and incompatible contracts fail before publication.
Existing generated files and manifests are preserved on validation failure.

`#setting($_ = $case_format('lc'))` may supplement the legacy header. It is
preserved as component case-format metadata and uses normal native output
encoding; linked contracts and explicit JSON tags are not rewritten.
