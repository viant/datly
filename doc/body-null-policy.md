# Present null whole-body inputs

An input parameter can opt into native literal JSON null decoding:

```sql
#define($_ = $View<*View>(body/).Required().WithBodyNullPolicy('empty-record'))
```

`spec.Parameter.BodyNullPolicy` is serialized as `bodyNullPolicy`; generation
emits `bodyNullPolicy=empty-record` in the parameter tag. Empty policy retains
strict Bindly decoding. `empty-record` is the only supported nonempty value.
The target must be a direct pointer to a struct on the whole body input.
Registration rejects selected body fields, other sources, output parameters,
other target families, codecs, source-type adaptations and transformers.

Only an explicitly supplied whole JSON null becomes a fresh empty record.
Missing and empty input still fails Required. All original record Has flags
remain false, with an allocated marker holder where the shape declares one;
the top-level input marker remains true. Ordinary object and nested-null
semantics are unchanged. JSON media recognition and raw replay stay owned by
Bindly. HTTP and MCP use the same compiled binding. Request body and MCP
argument presence remains required while the value schema permits null.
Anonymous flattened MCP bodies with the option are rejected explicitly.

An already bound nil record is normalized before dependent binding reads,
capture or input hooks only if Bindly's actual compiled presence marker is
available and true. Markerless nil Go fields cannot prove explicit suppliedness.
Existing strict BoundInput behavior is unchanged. Prepared records retain
identity and presence.

The built-in authored body date formatter requests `json.RawMessage` and
transforms it after Required. Bodies containing authored `timeLayout` or
`dateFormat` fields therefore explicitly reject this policy at registration.
Ordinary RFC-formatted `time.Time` fields without an authored layout keep the
ordinary native decoder and are supported. This feature does not infer any
client field, update identity, sequence allocation, validation or write policy.
