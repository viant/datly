# Canonical authoring skills and reproducible bundles

[All guides](README.md) · [Coverage](feature-skill-coverage.md)

The canonical `llm` source contains three application-authoring skills:
[datly-reader](../../llm/datly-reader/SKILL.md),
[datly-writer](../../llm/datly-writer/SKILL.md), and
[datly-custom-component](../../llm/datly-custom-component/SKILL.md).
Short entrypoints route to grammar, contracts, examples and operational references.
They describe the integrated authoring rules and explicitly documented remaining
gaps. See [release status](status.md) for the current validation boundaries.

## Source and product inputs

Source documentation uses a workspace with `datly/`, `xdatly/` and `llm/` as sibling
directories. Publish or copy those source roots together to preserve canonical-skill
links. Materialized skill bundles carry their referenced files within each skill
root and do not require that source workspace layout.

All links inside a canonical skill, including supporting references, resolve from
that skill's root. Preserve this convention; do not convert reference links to
Markdown-document-relative sibling paths. `packaging.json` declares exact product
files and selected directory indexes. It does not authorize recursive repository
imports or inclusion of maintainer history.

From the matching Datly module, with your canonical source and product workspace:

```sh
go run ./internal/cmd/skillpack -source ../llm -products .. -out ../skill-bundle -write
go run ./internal/cmd/skillpack -source ../llm -products .. -out ../skill-bundle
```

These paths use the release sibling layout. For the embedded bundle, run
`go run ./internal/cmd/skillpack -write`, then omit `-write` to check it. The output is disposable generation, not a live
installation. Canonical `llm/` imports come from `-source`; product `datly/` and
`xdatly/` imports come from `-products`. Retain the declared source files and
packaging profile together to reproduce identical bytes. Use the same tool for
embedded assets, selecting its normal embedded destination only in a reviewed
integration tree. Never hand-edit embedded copies.

## Validation and installation shape

Skillpack rejects missing/undeclared links, heading fragments, symlinks, root
escapes and invalid metadata. Product Markdown links become skill-root-relative
links under `references/product`; Go and go.mod references become text assets.
Imported skill entrypoints become `SKILL.reference.md` supporting documents.
Only the three actual roots retain `SKILL.md` entrypoints. The manifest records
source and generated hashes with no unresolved references.

A materialized filesystem bundle is self-contained. Raw canonical folders still
have declared product dependencies and are not the installation artifact.
Generating or validating a bundle does not install it or publish an MCP server.
Native Final SEP-2640 publication separately seals file bytes and inventory;
see [protocols](protocols.md#developer-mcp-and-final-sep-2640-skills).

Public product guides exclude host-specific audit paths and maintainer history.
Keep implementation evidence in a separate audit report. Update source docs and
canonical skills first, refresh the explicitly declared product inputs, then
regenerate and check the bundle after every accepted source change.
