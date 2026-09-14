# Datly guides

[Datly overview and runnable demo](references/product/datly/README.md)

Start with a working API, then load the detail needed for your application.
Examples marked **fragment** require the named application types or configured
services. Sections marked **pending** document a requested contract and do not
provide a runnable feature switch.

| Your task | Read |
| --- | --- |
| Understand DAO/service responsibilities, components, DQL, StructQL and generated code | [Programming model](references/product/datly/doc/programming-model.md) |
| Run the SQLite demo and understand its contract | [Quickstart](references/product/datly/doc/quickstart.md) |
| Understand the execution model and ownership | [Architecture](references/product/datly/doc/architecture.md) |
| Write DQL declarations, parameters, JOIN controls and typed CASTs | [DQL syntax and grammar](references/product/datly/doc/dql.md) · [EBNF](references/product/datly/doc/dql.ebnf) |
| Transcribe DQL, import types and preserve generated edits | [Authoring](references/product/datly/doc/authoring.md) |
| Understand reader/writer hook order, sparse updates and invariants | [Hook flow diagrams](references/product/datly/doc/hooks.md) |
| Build nested reads and paginated outputs | [Readers](references/product/datly/doc/readers.md) |
| Build analytical reports or compare query frames | [Reports](references/product/datly/doc/reports.md) |
| Generate a mutator and understand Has markers, SyncPresence and invariants | [Generated mutators](references/product/datly/doc/generated-mutator.md) |
| Write sparse updates and related inserts | [Mutations](references/product/datly/doc/mutations.md) |
| Return business errors or shape a typed/raw response | [Errors and custom output](references/product/datly/doc/errors-and-output.md) |
| Enforce verified identity and row access | [Security](references/product/datly/doc/security.md) |
| Register custom behavior, return bytes or finalize output | [Custom handlers](references/product/datly/doc/custom-handlers.md) |
| Expose HTTP and MCP | [Protocols](references/product/datly/doc/protocols.md) |
| Configure a linked server, resources and reload | [Configuration](references/product/datly/doc/configuration.md) |
| Select a cache and plan warmup | [Cache and warmup](references/product/datly/doc/cache-and-warmup.md) |
| Schedule jobs and handle uncertain completion | [Async](references/product/datly/doc/async.md) |
| Capture execution and export telemetry | [Observability](references/product/datly/doc/observability.md) |
| Publish OpenAPI/MCP descriptions and plan dictionaries | [API documentation](references/product/datly/doc/api-documentation.md) |
| Serve a configured content folder | [Static content status](references/product/datly/doc/static-content.md) |
| Use the authoring skills | [Skills](references/product/datly/doc/authoring-skills.md) |
| Check feature coverage across skills | [Feature-to-skill coverage](references/product/datly/doc/feature-skill-coverage.md) |
| Check implementation and verification boundaries | [Status and evidence](references/product/datly/doc/status.md) |

For a new data API: quickstart → readers → security → protocols. For a mutation:
authoring → mutations → custom handlers. Before deployment: configuration →
cache/async/observability as needed → status and evidence.

See [project init/build](references/product/datly/doc/project-build.md) and [view selectors and formats](references/product/datly/doc/selectors-and-formats.md) for automatic discovery, exact-name policy and download contracts.
