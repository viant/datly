# Linked Go factories

Custom projects use `datly init` and `datly build` through `project/build.Service`.
The build discovers component metadata and reachable shapes, then internally
links exported Contract and mutation Definition factories into the existing
`x.Registry`. Application packages need no init(), Register call, or maintained
import list. See `project/build/README.md` for module/workspace and resource rules.

Generated `<component>_link_gen.go` companions remain available for applications
that explicitly assemble an embedded registry. They are optional; automatic
project builds use the factory declaration itself and the established typed
custom/mutation adapter constructors. Policy source stays runtime-free.

Pass that registry to `report.ProjectConfig.Registry`. `CompileArtifacts`
snapshots it once for the whole stage, compiles all components, and
`RuntimeComponents` consumes the resolved handlers. Standalone compilation can
use `bootstrap.NewArtifactBuilder(registry)`, `Build`, and
`Artifact.Registration`. The registration method owns canonical metadata while
the application supplies database/read capabilities and providers. Explicit
handlers cannot override a resolved named factory.

Factory references retain full package authority. Unqualified names use the
component package, not an unrelated input type's default package. A blank route
handler inherits the component's single resolved handler; conflicting nonblank
names fail. Missing factories, invalid signatures, nil results and mismatched
input/output contracts fail before publication. Package exposure is unchanged:
linked exports do not create routes or expose private component dependencies.

`viant/x` owns compiled exports, native callable type identity, invocation and
registry snapshots. Its existing `Registry.Merge` remains type-only; callable
batches merge explicitly through `RegisterFunctions(source.Functions()...)`.
Snapshots detach tables and type ASTs, not arbitrary closure state. Factories
must not capture mutable active-generation state. A definition is constructed
per component build; its mutation Programs remain invocation-local.

This is compiled Go linking, not dynamic source execution. Reflection cannot
discover free functions from a package name. Original Datly's
`repository/handler/handler.go` resolves registered handler/factory **types**;
its plugin snapshot loads exported type lists or an extension registry. Neither
mechanism supplies callable top-level function values to the new runtime.
Non-persisted synthetic/DQL mutation execution remains a separate requirement;
registering compiled exports does not implement it.
