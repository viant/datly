# Named connector aliases

Use `AliasOf` when reused components declare different connector names but must
participate in the same managed database transaction:

```yaml
Connector: studio
Connectors:
  - Name: studio
    Driver: sqlite
    DSN: file:studio.db?cache=shared
  - Name: authz
    AliasOf: studio
```

Both names resolve to the same database handle, connection pool and Datly
transaction identity. This also works for MySQL: configure the driver, DSN,
credentials and pool on the target only. An alias grants no authorization and
does not combine independent databases into a distributed transaction.

Aliases may precede their targets and may refer to another alias. Unknown
targets, cycles, duplicate names and aliases with their own connection, secret
or pool settings are rejected. The target handle is closed once by its owning
connector set. Independently configured connectors remain independent even
when their DSNs happen to match.
