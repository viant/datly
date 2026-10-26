# Compact skill and resource discovery

`Discover` is an additive catalog helper used by `skills_discover`. Hosts supply
only caller-authorized manifests. Native skills/list, skills/get, and resource
contents remain unchanged. No file contents are fetched by discovery.

| Input | Meaning |
| --- | --- |
| kind | skills (default) or resources |
| query | Case insensitive substring of name, description, group and URI in skills mode; resource URI in resources mode |
| group | Exact frontmatter group, falling back to skill name |
| skillURI | Exact skill entrypoint URI, resources mode only |
| prefix | Exact resource URI prefix, resources mode only |
| limit | Page size 1–100; omitted or zero defaults to 32 |
| cursor | Next page; reuse the same mode, filters and page size |

Default summaries contain URI, name, description, group, resource count and
dynamic flag. Resources mode returns only matching URI/digest/size descriptors.
Groups and total counts apply before pagination. Missing groups or skill URIs
return empty results. Dynamic skills have no static descriptors to enumerate.

Read a selected entrypoint and relevant indexes directly through resources/read
or the host's compatibility reader. Use resources mode to find a particular
contract by URI, then read only matching files. Use skills/get only when the
complete inventory is needed.

Cursors bind normalized filters, mode, page size, and the entire caller-visible
catalog including resource digests. Changes to the visible catalog invalidate
cursors. Results have private cache scope and zero TTL. Hosts must apply normal
discovery and description authorization checks before calling the helper.
