# Handler-Only DQL Transcription

Use `Generator{Operation: "handler"}` to generate a registration for an existing
native Go handler. `get` selects reader generation; `post`, `put`, and `patch`
select mutation generation except for the explicit SQL-free POST factory branch below. The authored HTTP method does not select execution
semantics.

## Source-Authored Factory

New registrations can declare the native factory directly, without compiling
that factory or its contracts into the transcription executable:

```sql
#package('example.com/app/generated/convert')
#import('conversion','example.com/app/conversion')
#setting($_ = $route('/convert','POST'))
#setting($_ = $handler_factory('conversion.NewConvert','Convert'))
#setting($_ = $input_type('conversion.Input'))
#setting($_ = $output_type('conversion.Output'))
#setting($_ = $mcp('Convert'))
#setting($_ = $case_format('lc'))
```


Use `transcribe handler`, or the same discovery/generator API below without
`HandlerBindings`. `handler_factory` selects source-backed typed handler generation without a JSON header. Its first argument is the qualified factory (full package or import alias); the optional second argument preserves the component name. Otherwise the source name is used. It requires one route method and explicit qualified input/output types. Duplicate factories, modifiers, missing contracts and mixing the directive with a legacy handler header fail.
`#package` is mandatory destination authority; constructors and connectors are
never inferred. Qualified symbols may use explicit DQL import aliases. Route, connector, MCP exposure, API key, visibility, casing and generation destinations use ordinary DQL settings. Binding declarations must agree with the linked source contract; generated body-shape authoring remains a distinct required stage.

Datly uses Go's build-selected export data to validate the actual function and
contract identities. No reflection fallback, application-wide registry, or
constructor execution is needed. Contracts resolve to exported nongeneric named
structs; genuine aliases to those structs are accepted and emitted using their
underlying named identity. DQL parameter declarations must agree with the source
contract's bindings, types, optionality, codec/source/destination types, safe error
status, and existing serialization tags. Output declarations are checked against
the output contract. Matching declarations remain canonical component metadata;
conflicting codec/status/type/tag/provider changes fail before publication. Remove obsolete declarations after
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
leave existing generated and application files unchanged.
Metadata reload uses the same build-selected source files as validation.
Source-authored factories retain destination build validation through the same
staged publication path as every other generation entry point. Generated files
are replaceable; separate application files and linked contracts are preserved.
No package ownership sidecar is read or written.

Existing compiled mappings remain supported. If both forms select a legacy
`Type`, factory, destination, and actual contract identities must agree.

## Generated SQL-free POST contracts

A canonical `handler_factory` can select native `transcribe post` when the
factory and both Input/Output contracts belong to the explicit destination
package. Declare the local generated contract names through `input_type` and
`output_type`; existing DQL input/output declarations determine their fields.
Existing logical Go types may be referenced through their canonical imports.

```sql
#package('example.com/app/archive')
#import('archive','example.com/app/archive')
#setting($_ = $route('/archive','POST'))
#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $input_type('ArchiveInput'))
#setting($_ = $output_type('ArchiveOutput'))
#define($_ = $Data<*archive.ArchiveRequest>(body/data).Optional())
#define($_ = $Status<string>(output/status))
#define($_ = $Results<[]*archive.ResultItem>(output/body).WithTag('json:"results"'))
```

The authored `NewArchive` must be an exported top-level function with exact
signature `func() handler.Contract[ArchiveInput, ArchiveOutput]`. Its generated
contracts need not exist before the first generation. Build-selected declaration
metadata checks the factory's function identity; existing staged Go overlay
validation checks its exact generic contract types before publication. The
factory and package initialization are never executed during generation.
Regeneration replaces generated contracts and preserves separate business files.

This branch accepts only native POST generation and contains no SQL, reader
views, physical mutation root, or generated mutation handler. Typed component
dependencies own reads and generated child writers own persistence. Their normal
invocation scope owns the shared unit; a parent handler does not acquire manual
DML or application-controlled commit/rollback through this generation feature.
Imported source contracts keep the existing `transcribe handler` behavior above.

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
Normal staged publication applies. Regeneration replaces direct generated-file
edits and preserves separate application hooks and linked contracts.

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
Existing generated and application files are preserved on validation failure.

`#setting($_ = $case_format('lc'))` may supplement the legacy header. It is
preserved as component case-format metadata and uses normal native output
encoding; linked contracts and explicit JSON tags are not rewritten.

The generated POST branch requires unqualified exported `input_type` and `output_type` names. Qualified authored or imported contract references retain the existing linked source-backed handler path and its destination checks; they never opt into generated ownership.
