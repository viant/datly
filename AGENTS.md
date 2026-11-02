# Project instructions

## Capability reuse and explicit user approval

- Before adding any feature or mechanism, inspect the owning framework's existing APIs, configuration, documentation, implementation, tests, and callers at the exact version used by the application. Prove the relevant native behavior with an executable example before proposing an alternative.
- Trace the existing path across layers and identify its owner. Reuse that path; remove demonstrated duplication instead of adding parallel wrappers, adapters, fallback paths, caches, validation, permission checks, or configuration switches.
- Before implementing a new feature or framework extension, consult the user with the existing options, evidence of the remaining gap, the smallest proposed change and owning layer, and compatibility implications. Obtain explicit approval of that approach first. A request to fix a regression is not approval to invent a new mechanism. Agent or reviewer recommendations do not substitute for user approval.
- Keep validation, strict/lenient argument handling, authorization and contract enforcement in the framework or producer that already owns them. Do not silently replace native lenient behavior with stricter application, host, proxy or UI rejection.
- Passing tests does not justify a redundant feature. Preserve existing contracts and add regressions through the real native path. Do not introduce a new API, flag, gate, compatibility layer or workaround solely to make a test pass.
- Carry these requirements into delegated work. When the user has already approved the exact approach, continue within that authorization; ask again only if the proposed approach or scope changes.

