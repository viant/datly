# Project builds and deployment

Use `datly init` to scaffold and `datly build` to discover Go-selected component
packages, reachable types and typed factories. Discovery/linking is internal;
do not require application `init()`, `Register()`, blank-import lists or aggregate
registration functions. Ordinary imports express real code dependencies.

DQL transcription remains explicit before building generated Go components.
Preserve authored hooks, resource manifests, Go workspace/tags/target settings
and existing module choices. New packages are found by normal traversal; runtime
exposure is a separate policy. Never infer aliases to repair discovery errors.

The main Datly module remains a local `v1` checkout. The release copy pins the
published SDK `v0.5.4-0.20260914204318-08752d9972c1` and native dependencies.
Use a `-local` mapping for Datly and exact published `-pin` values for dependencies.
`v0.0.0` is only a local requirement placeholder paired with a replacement, not
a published release. Dependency replacements are not inherited by Go: supply the
complete selected graph in the application module/workspace.

These binaries remain source-backed: deploy the corresponding metadata/source
and resource files at the recorded paths, or rebuild for the deployment layout.
An embedded SQL/static/doc/MCP resource proof does not establish source-free
custom-project deployment. Do not promise that copying only the binary works.

Use the [project guide](references/product/datly/doc/project-build.md) for exact
commands, factory signatures, preservation and deployment boundaries, and the
[quickstart](references/product/datly/doc/quickstart.md) for the runnable snapshot demo.

Parent real-TCP acceptance passes custom-build read/mutation endpoints and
add/remove package rebuilds. Do not present the documentation author's sandbox
listener restriction as a current implementation or parent-acceptance gap.
The source-backed deployment requirements above still apply.
