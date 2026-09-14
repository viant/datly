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

The development snapshots used temporary `xdatly_1` and `datly_1` names. Their
release copies use the canonical module paths above. A branch named `v1` does
not add `/v1` to Go imports. xdatly is one module; the runtime depends on this SDK,
and the SDK must not depend on the runtime.

## Publication order

1. Review the SDK migration and run its full tests and dependency-boundary check.
2. Commit the SDK changes on `v1`; push that branch when ready for consumers.
3. Resolve the pushed SDK commit to its real Go module version and pin that
   version in Datly's `v1` branch. Keep local development replacements out of
   release module manifests.
4. Run Datly's regression and generated-component checks against the published
   SDK and native dependencies, then commit and push its `v1` branch.
5. Create release tags only after the release gates pass. Branch publication and
   a version tag are separate actions; no `v1.0.0` tag is implied by this document.

To publish the prepared local branch explicitly, use `git push -u origin v1`
from its worktree. The current migration does not push or create tags itself.
For a matched local workspace before publication, use an uncommitted `go.work`
or local replacement, and verify again against published versions before release.
