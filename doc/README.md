# Datly guides

[Datly overview and runnable demo](../README.md)

Start with a working API, then load the detail needed for your application.
Examples marked **fragment** require the named application types or configured
services. Sections marked **pending** document a requested contract and do not
provide a runnable feature switch.

| Your task | Read |
| --- | --- |
| Run the SQLite demo and understand its contract | [Quickstart](quickstart.md) |
| Understand the execution model and ownership | [Architecture](architecture.md) |
| Write DQL declarations, parameters, JOIN controls and typed CASTs | [DQL syntax and grammar](dql.md) · [EBNF](dql.ebnf) |
| Transcribe DQL, import types and preserve generated edits | [Authoring](authoring.md) |
| Understand reader/writer hook order, sparse updates and invariants | [Hook flow diagrams](hooks.md) |
| Build nested reads and paginated outputs | [Readers](readers.md) |
| Build analytical reports or compare query frames | [Reports](reports.md) |
| Generate a mutator and understand Has markers, SyncPresence and invariants | [Generated mutators](generated-mutator.md) |
| Write sparse updates and related inserts | [Mutations](mutations.md) |
| Return business errors or shape a typed/raw response | [Errors and custom output](errors-and-output.md) |
| Enforce verified identity and row access | [Security](security.md) |
| Register custom behavior, return bytes or finalize output | [Custom handlers](custom-handlers.md) |
| Expose HTTP and MCP | [Protocols](protocols.md) |
| Configure a linked server, resources and reload | [Configuration](configuration.md) |
| Select a cache and plan warmup | [Cache and warmup](cache-and-warmup.md) |
| Schedule jobs and handle uncertain completion | [Async](async.md) |
| Capture execution and export telemetry | [Observability](observability.md) |
| Publish OpenAPI/MCP descriptions and plan dictionaries | [API documentation](api-documentation.md) |
| Serve a configured content folder | [Static content status](static-content.md) |
| Use the authoring skills | [Skills](authoring-skills.md) |
| Check feature coverage across skills | [Feature-to-skill coverage](feature-skill-coverage.md) |
| Check implementation and verification boundaries | [Status and evidence](status.md) |

For a new data API: quickstart → readers → security → protocols. For a mutation:
authoring → mutations → custom handlers. Before deployment: configuration →
cache/async/observability as needed → status and evidence.

See [project init/build](project-build.md) and [view selectors and formats](selectors-and-formats.md) for automatic discovery, exact-name policy and download contracts.
