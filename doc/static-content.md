# Static content from an explicit resource root

[All guides](README.md) · [Configuration](configuration.md)

The candidate maps local or embedded resource folders to HTTP prefixes through
immutable Manager generations. External content uses the existing AFS owner.
This feature is integrated in the local `v1` release copy; final release regression remains required.

## Declare content

```sql
#setting($_ = $route('/site', 'GET'))
#setting($_ = $api_key('X-Static', 'application-configured-secret'))
#setting($_ = $static_resource('site', 'public'))
```

This fragment selects registered namespace `site`, root `public` and an explicit
HTTP prefix. Replace the example key through your application's configuration
policy. `$static_content('content-url', 'root')` instead selects source-backed
content. Both arguments are quoted literals. Static declarations reject SQL,
parameters, handlers and mixed component metadata; they are static components,
not reader views.

Generation emits `DatlyStaticContent()`, `DatlyResources embed.FS` and
`DatlyResourceNamespace`. The package resource loader shares the canonical
Bindly store with SQL, templates and documentation. Use the custom build's
internal discovery for application linking; do not hand-maintain import lists.
Missing resources fail before publication.

## Local authority and reload

A configured ContentURL cannot authorize its own filesystem root. Default
absolute local paths are checked component by component from the volume root;
relative paths use the process working directory as independently selected
authority. Symlink components and parent traversal fail, including platform
alias directories in an absolute path. Empty `file:` URLs fail rather than
selecting the current directory.

Trusted application bootstrap may supply an independently opened, caller-owned
`*os.Root` through `config.Loader.StaticLocalRoot` or
`gateway.Config.StaticLocalRoot`. Content paths must then be relative to that
handle. JSON cannot supply this capability. Keep the handle open through the
server's reload lifetime and close it after shutdown. Never derive an unchecked
root from the untrusted ContentURL itself. The selected directory remains pinned
during snapshotting; multi-file edits are not an atomic source transaction.

The programmatic root is configured by trusted application bootstrap. Existing HTTP policy owns route
precedence, API keys, CORS, GET/HEAD, ranges and errors. Failed snapshot/reload
leaves the prior generation serving. File edits do not mutate published bytes.

## Deployment and verification

Generated static resource tests serve nested/binary/hidden files after removing
source and generated asset directories. This proves those generated embedded
assets; [source-backed project builds](project-build.md#deployment-contract)
still need their metadata/source deployment inputs.

Verify index/media behavior, GET/HEAD/ranges, missing files, escaped paths,
symlinks, policy denial, route collisions and failed/successful reload. Test the
actual AFS provider and credentials before claiming remote-content deployment.
