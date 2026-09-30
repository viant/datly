# Linked codec dependencies

Components can name linked codec types without an application codec map:

```go
Values []int `parameter:"Values,kind=query,in=value,dataType=[]string" codec:"example.com/app/codecs.QueryList"`
```

The referenced type must implement `codec.Factory` or `codec.Instance` through
its pointer method set. Instances cannot take factory arguments. Existing codec
construction, source/destination types, resources and invocation options remain
unchanged.

Resolution order is built-in, catalog type, then application factory fallback.
Dependency loading follows only referenced qualified names. It does not import
every linked type or register unrelated components/resources. Existing catalog
authority and compiled-identity conflicts remain authoritative. A recognized
type with an invalid configuration fails; it does not fall through to another
factory. Unrecognized names may still be handled by an application factory.

DQL import aliases are canonicalized before generated parameter/column tags are
emitted. Source-backed discovery preserves referenced source declarations and
manifest ownership. Source-free eager bootstrap uses compiled identities only;
use canonical names in reflected tags, not guessed package-basename aliases.
Short names retain existing catalog/factory semantics; they do not trigger a
search of every linked package.

The implementation must already be linked into the executable. After authoring
a new codec, run `datly link sync` with its package selected and rebuild. Runtime
catalogs are invocation-independent, artifact-local snapshots; child and summary
row codecs are included without changing execution or publication policy.
