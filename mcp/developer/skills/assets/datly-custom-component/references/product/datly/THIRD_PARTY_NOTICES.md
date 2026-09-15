# Licensing and attribution

The repository's original Apache-2.0 [LICENSE](LICENSE) and Viant [NOTICE](NOTICE)
are retained unchanged in this migration. Preserve them when redistributing Datly.

The OpenAPI model port includes its own
[LICENSE](gateway/openapi/openapi3/LICENSE) and
[NOTICE](gateway/openapi/openapi3/NOTICE). The HTTP adapter attribution is retained
in [OPENAPI_NOTICE](gateway/http/OPENAPI_NOTICE). These files record the source
and applicable attribution; do not remove them when repackaging those files.

The separate `github.com/viant/xdatly` SDK and other dependencies retain their own
licenses. `go.mod` and `go.sum` identify selected versions; `go list -m all` gives
the resolved module inventory. Those files are not a consolidated license grant.
When distributing a binary, review and include the notices required by the actual
dependency set, including optional drivers and integrations used by that build.

Generated authoring bundles include selected Datly and xdatly documentation and
source excerpts. The packaging declaration carries Datly's LICENSE and NOTICE and the SDK LICENSE into every skill, together with source and generated-file hashes. Retain
those notices with the generated bundle. This document does not assign a new
license to independently authored application code.
