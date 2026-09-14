# Authoring Skill Packaging

The three authoring source folders use skill-root-relative Markdown references,
including references inside supporting documents. Do not replace them with
document-relative sibling paths. `packaging.json` is the exact approved product
reference import list; it is not a glob or a request to copy a repository.

From the Datly module, materialize an installable filesystem bundle:

```sh
GOWORK=off go run ./internal/cmd/skillpack -source llm -products /path/to/workspace -out /path/to/installed-skills -write
```

The workspace contains sibling `datly` and `xdatly` source trees. Canonical
release skills are distributed inside the Datly repository at `datly/llm`; no
sibling `llm` checkout is required. The xdatly product sources must match commit
`08752d9972c16da25047f5f0ad979c2894f7d5ff`, the SDK version pinned in Datly.
Canonical `llm/` imports are always resolved from `-source`. The output contains
three complete skill folders, usable without any source checkout. Use this
materialized output for filesystem skill installation; raw source folders have
declared product dependencies and are not the installation artifact.

Use the default `-out` to generate Datly's embedded bundle. Omit `-write` to verify
an existing output byte-for-byte. The same packaging path serves both targets;
never hand-edit the generated copies.

Product Markdown links are rewritten at parser-confirmed destination spans into
the selected skill's root. Source code references retain raw bytes as `.go.txt`
assets, so an install or Go package walk cannot mistake them for application code.
Source hashes and generated file hashes are recorded in the manifest. Imported
skill entrypoints are named `SKILL.reference.md`, so filesystem discovery does not
mistake them for extra installed skills. They remain ordinary supporting
documents; reading them grants no activation or tools.

Only exact approved files are imported. Nine explicit indexes expose selected
API references without exporting the corresponding source directories. The public
README profile starts at the product introduction after the website badges and
ends before contributor-workflow navigation and states that
boundary in the rendered product guide; private working-history and host paths
are not followed. Historical implementation imports are replaced by maintained public guide links;
maintainer-only evidence is excluded from the product import declaration.

Unapproved links, missing files, escaping paths, symlinks, unsupported link forms,
missing heading fragments and invalid skill metadata fail packaging. There is no
unresolved-reference warning mode. Keep the complete source/profile together and
re-run packaging when product documentation changes.

Readiness: exact-name/user-defined-alias/duplicate-output rules and standalone
async configuration are integrated in the local `v1` release copy. Final release
regression is still required. DQL destination and native recursive Velty work
remain separate pending integrations; consult the imported product status guide.

Each materialized skill carries Datly LICENSE/NOTICE and xdatly LICENSE files from
the exact declared product sources. Preserve those files when redistributing.
