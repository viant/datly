# Authoring Skill Packaging

Canonical reader/custom Markdown uses document-relative links: a reference
beside `reader-examples.md` links to `reader-examples.md`, not
`references/reader-examples.md`. Product dependency links are also relative to
the containing document; their `references/product/` targets are materialized
from the exact import list below.

`SkillRootLinks` explicitly identifies the untouched writer source as using
skill-root-relative input links during its separate authoring update. The
packager interprets that declared input base and emits document-relative links
for every installed/embedded skill, including writer copies and imported docs.
Do not hand-edit generated links or infer a source link base from file existence. `packaging.json` is the exact approved product
reference import list; it is not a glob or a request to copy a repository.

From the Datly module, materialize an installable filesystem bundle:

```sh
GOWORK=off go run ./internal/cmd/skillpack -source llm -products /path/to/workspace -out /path/to/installed-skills -write
```

The workspace contains sibling `datly` and `xdatly` source trees. Canonical
release skills are distributed inside the Datly repository at `datly/llm`; no
sibling `llm` checkout is required. The imported xdatly files must match commit
`08752d9972c16da25047f5f0ad979c2894f7d5ff`, the SDK version pinned in Datly.
Canonical `llm/` imports are always resolved from `-source`. The output contains
three complete skill folders, usable without any source checkout. Use this
materialized output for filesystem skill installation; raw source folders have
declared product dependencies and are not the installation artifact.

Use the default `-out` to generate Datly's embedded bundle. Omit `-write` to verify
an existing output byte-for-byte. The same packaging path serves both targets;
never hand-edit the generated copies.

Product Markdown links are rewritten at parser-confirmed destination spans into
paths relative to the containing generated document. Source hashes and generated file hashes are recorded
in the manifest. Exact canonical cross-skill imports keep their maintained source
identity and do not grant activation or tools. Only the linked reference closure
is imported; there are no extra skill entrypoints or source-code assets.

Only exact approved files are imported. The profile selects maintained sections
for warmup cases, selectors/formats, shared YAML documentation, static roots,
report configuration and telemetry lifetime. `StartAt`/`EndBefore` boundaries
are declared in `packaging.json`; imported bytes are never hand-edited. The
remaining application contracts live in the existing canonical skill references.
Cross-skill references use exact `llm/` imports resolved from `-source`.

No runtime, compiler, test-source directories, root README, broad public overview,
legacy writer recipe or untracked tutorial is included. Runtime source remains
available separately in the product repository. This author-facing bundle teaches
declarative graphs and generated pure Go, with business behavior in Go hooks.
There are no source directory indexes or procedural authoring alternatives.

Unapproved links, missing files, escaping paths, symlinks, unsupported link forms,
missing heading fragments and invalid skill metadata fail packaging. Link
validation resolves each destination from the actual containing file's directory,
including nested imported documents; a same-named file at the skill root is not
a substitute. Raw source product dependencies require materialization; their
canonical source identity is checked against the exact declared imports. There is no
unresolved-reference warning mode. Keep the complete source/profile together and
re-run packaging when product documentation changes.

Readiness: the v1 CLI uses `datly transcribe get|patch|post|put`, with Go output
by default. Developer MCP targets declare their configured generation operation. Skills require discovery of the connected capability
and reporting missing `transcribe`; lower-level transcription is not a substitute.
Packaging validates documents and metadata, not generated endpoint behavior.

Each materialized skill carries Datly LICENSE/NOTICE and xdatly LICENSE files from
the exact declared product sources. Preserve those files when redistributing.
