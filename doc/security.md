# Verified JWT input and parameterized authorization

[All guides](README.md) · [HTTP and MCP](protocols.md)

Declare the identity information each component needs, verify it through the
configured codec, and consume that typed input in authorization predicates.
Datly does not add an ambient JWT variable or principal to every component.
Decoding a token is not verification, and a client-selected tenant is not proof
of access to that tenant.

## Configure the trust source

`JWTValidator` uses `github.com/viant/scy/auth/jwt/verifier.Config`, preserving
original Datly's certificate/JWKS URL or RSA public-key resource configuration.
Use the issuer/trust source chosen by your application; never derive a trusted
key URL from untrusted request content.

For an embedding host, `runtime/auth.New(ctx, &auth.Config{JWTValidator: config})`
returns the codec factory supplied through `bootstrap.ArtifactInput.CodecFactory`.
The linked standalone host accepts `JWTValidator` configuration and installs the
same declared-input adapter. Validate certificate refresh/key rotation against
the pinned native dependency as well as ordinary token acceptance.

## Declare and consume claims

This is a Go fragment using your actual registered predicate type identity:

```go
import (
    "context"
    "fmt"
    "github.com/viant/scy/auth/jwt"
    "github.com/viant/xdatly/predicate"
)

type Input struct {
    JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.com/app/security.Authorization" json:"-"`
}
type Authorization struct {
    Input *Input `bind:"kind=input,required"`
}
func (p *Authorization) Compute(ctx context.Context, _ any) (*predicate.Criteria, error) {
    if err := ctx.Err(); err != nil { return nil, err }
    if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject == "" {
        return nil, fmt.Errorf("authorization required")
    }
    claims := p.Input.JWT
    return &predicate.Criteria{
        Expression: "owner_id = ? AND owner_subject = ?",
        Placeholders: []any{claims.UserID, claims.Subject},
    }, nil
}
```

Register the predicate and codec through the application's existing typed
registration owners, and use a query that consumes its predicate group. The
component declaration and query are deliberately omitted here: these ownership
columns are application-specific. The [full authored reference](../../llm/datly-reader/references/tags-and-interfaces.md#jwt-input-and-authorization-predicates)
explains the expected scope.

`Subject` is the native registered claim; `UserID` is a custom claim whose
meaning and required presence come from your issuer contract. Keep claims out
of response JSON. Missing/invalid/expired credentials and predicate denial must
stop protected SQL. If access denial must produce a typed error instead of an
empty filtered result, author that policy explicitly.

## SQL and template scope

Use placeholders and ordered arguments for values, including claim values.
Never interpolate JWT strings, user criteria or column names into SQL. Selectors
must restrict allowed fields/order/filter methods. Composite predicates must
match complete tuples, not independent IN lists that allow cross-combinations.

A predicate template receives `FilterValue`. When attached to the JWT input it
can use that value's claims; an unrelated template does not gain global `$JWT`
or arbitrary component inputs. A typed Go predicate requests `kind=input` as
above. Authored SQL and predicates share the pure SQL fragment owner for SQL and
ordered arguments, not a database/session facade.

## Internal dependencies and protocols

Retain the original request through HTTP adaptation so declared header inputs
can bind correctly. A private component may be callable internally while absent
from HTTP routes/MCP tools; privacy is not an authorization bypass. Explicit
trusted typed input forwarding is a separate operation and must not be exposed
as a client-controlled service override.

MCP transport authorization, HTTP API keys, JWT input verification and business
row predicates serve different purposes. An authenticated MCP connection does
not silently create a component JWT input. OpenAPI document access is also a
separate policy from the operations it describes.

For async jobs, serialized claims/user IDs do not establish current authorization.
The job authorizer must refresh current access. See [async limits](async.md).

## Acceptance

Test valid UserID/Subject filtering, wrong signature, expired token, missing
credential, denied business identity and zero protected-table reads on failure.
Test a component without JWT input to prove no implicit dependency appears.
Certificate/public-key verification and predicate execution need separate proof.
Preserve authored required/error codes and safe public payloads through both HTTP
and MCP; private cause text must not replace an explicitly empty public message.
