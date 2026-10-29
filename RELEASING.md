# Release branches and module identity

Keep the existing xdatly `main` and Datly `master` branches available for their
current releases. Development of the new implementation takes place on a `v1`
branch in each repository. Separate worktrees keep those changes independent.
Do not merge the new implementation into the existing release branches as part
of this migration.

| Repository | Existing release branch | New branch | Go module |
| --- | --- | --- | --- |
| viant/xdatly | main | v1 | github.com/viant/xdatly |
| viant/datly | master | v1 | github.com/viant/datly |

Use the canonical module paths above. A branch named `v1` does not add `/v1`
to Go imports. xdatly is one module; the runtime depends on this SDK,
and the SDK must not depend on the runtime.

## Publication order

1. Review the SDK migration and run its full tests and dependency-boundary check.
2. Commit the SDK changes on `v1`; push that branch when ready for consumers.
3. Resolve the pushed SDK commit to its real Go module version and pin that
   version in Datly's `v1` branch.
4. Run Datly's regression and generated-component checks against the published
   SDK and native dependencies, then commit and push its `v1` branch.
5. Create release tags only after the release gates pass. Branch publication and
   a version tag are separate actions; no `v1.0.0` tag is implied by this document.

To publish the prepared local branch explicitly, use `git push -u origin v1`
from its worktree. The current migration does not push or create tags itself.

## Native report time input compatibility

Unformatted source `time.Time` query/form filters exposed in a cube JSON body
now require RFC 3339 values, matching their source conversion contract. A client
that previously sent a date-only value to an unformatted cube filter must send
a timestamp or use an explicitly authored date format. Malformed values return
an input error rather than disappearing from the filter. Explicit layouts and
codecs retain their contracts; linked typed `*time.Time` shapes are supported.
A linked `*any` field is not a substitute for an unformatted time source and is
rejected during report compilation. Existing explicitly formatted `*any`
filter fixtures remain covered separately.
