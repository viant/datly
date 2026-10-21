# Canonical authoring skills and reproducible bundles

[All guides](README.md) · [Coverage](feature-skill-coverage.md)

The canonical `llm` source contains three application-authoring skills:
[datly-reader](../llm/datly-reader/SKILL.md),
[datly-writer](../llm/datly-writer/SKILL.md), and
[datly-custom-component](../llm/datly-custom-component/SKILL.md).
Short entrypoints route to grammar, contracts, examples and operational references.
They describe the integrated authoring rules and explicitly documented remaining
gaps. See [release status](status.md) for the current validation boundaries.
For explicit verified identity, typed context dependencies and remote HTTP/MCP
provider requirements, see the [auth-context guide](auth-context.md). Its
generic provider registration syntax remains subject to connected-build verification.

## Source and product inputs

Canonical release sources are distributed in this repository's `llm/` directory.
The sibling release staging copy is not required to build or install the published
bundle. Edit the repository sources and product guides, then regenerate.

Product inputs use `datly/` and `xdatly/` sibling source trees. Select the
xdatly source from the dependency actually used by Datly, including any replacement:

```sh
GOWORK=off go list -mod=readonly -m -json github.com/viant/xdatly
```

Use that exact source as the workspace's `xdatly` input; a different sibling
checkout is not version authority. The approved SDK references are read-only
packaging inputs. Clone or copy this repository as `datly` in the workspace;
no sibling `llm` directory is needed. Materialized bundles contain their references
and need neither checkout at installation or runtime.

Canonical skill Markdown uses document-relative links, including supporting
references and exact product imports. Resolve each destination from its containing
document, as specified in [the packaging procedure](../llm/PACKAGING.md).
`packaging.json` declares exact product files and selected sections; it does not
authorize recursive repository imports or inclusion of maintainer history.

From the matching Datly module, with your canonical source and product workspace:

```sh
GOWORK=off go run ./internal/cmd/skillpack -source llm -products .. -out /tmp/datly-skill-bundle -write
GOWORK=off go run ./internal/cmd/skillpack -source llm -products .. -out /tmp/datly-skill-bundle
```

Canonical skills live in this repository under `llm/`. The `-products ..` path
selects sibling Datly and xdatly product checkouts; no sibling llm checkout is
required. For the embedded bundle, run
`GOWORK=off go run ./internal/cmd/skillpack -write`, then omit `-write` to check it.
The output is disposable generation, not a live installation. Canonical `llm/` imports come from `-source`; product `datly/` and
`xdatly/` imports come from `-products`. Retain the declared source files and
packaging profile together to reproduce identical bytes. Use the same tool for
embedded assets, selecting its normal embedded destination only in a reviewed
integration tree. Never hand-edit embedded copies.

## Validation and installation shape

Skillpack rejects missing/undeclared links, heading fragments, symlinks, root
escapes and invalid metadata. Product Markdown links are rewritten to
document-relative destinations under `references/product`; Go and go.mod references become text assets.
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
