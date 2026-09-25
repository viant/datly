package handleronly

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"

	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

var Constructions atomic.Int32

type Input struct {
	Request   *Request `parameter:",kind=body,in=request"`
	Token     string   `parameter:",kind=header,in=Authorization"`
	Inclusion string   `parameter:",kind=body,in=inclusion"`
	Exclusion string   `parameter:",kind=body,in=exclusion"`
	Debug     bool     `parameter:",kind=form,in=debug"`
	Tenant    int      `parameter:"TenantID,kind=query,in=tenant_id"`
}

type Request struct {
	Values map[string][]*Value `json:"values"`
}

type Value struct {
	Name string `json:"name"`
}

func (i *Input) Init(context.Context) error {
	if i.Inclusion == "" {
		i.Inclusion = "default"
	}
	return nil
}

type Alias = Input
type Different Input

type Output struct{ Value string }

func (o Output) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"converted": o.Value})
}

type Handler struct{}

func NewConvert() handler.Contract[Input, Output] {
	Constructions.Add(1)
	return &Handler{}
}

func (*Handler) Exec(_ context.Context, _ handler.Session, in *Input, out *Output) error {
	if in.Token != "allowed" && in.Token != "Bearer allowed" {
		return &response.Error{Code: 401, Cause: errors.New("authentication required")}
	}
	if in.Inclusion == "fail" {
		return &response.Error{Code: 400, Cause: errors.New("invalid inclusion")}
	}
	out.Value = in.Inclusion + ":" + in.Exclusion
	if in.Debug {
		out.Value += ":debug"
	}
	return nil
}

// A structurally identical, unrelated interface is not xdatly's Contract.
type Contract[I, O any] interface {
	Exec(context.Context, handler.Session, *I, *O) error
}

func WrongContract() Contract[Input, Output]          { return &Handler{} }
func WrongInput() handler.Contract[Different, Output] { panic("must not execute") }

type CaseOutput struct {
	AccountInfo *AccountInfo
	Accounts    []AccountInfo
	Missing     *AccountInfo
}

type AccountInfo struct {
	AccountName string
	Enabled     bool
	RecordCount int
	Explicit    string `json:"KeepThisName"`
}

type CaseHandler struct{}

func NewCaseHandler() handler.Contract[Input, CaseOutput] {
	Constructions.Add(1)
	return &CaseHandler{}
}

func (*CaseHandler) Exec(ctx context.Context, session handler.Session, in *Input, out *CaseOutput) error {
	var converted Output
	if err := (&Handler{}).Exec(ctx, session, in, &converted); err != nil {
		return err
	}
	account := AccountInfo{AccountName: converted.Value, Explicit: "explicit"}
	out.AccountInfo = &account
	out.Accounts = []AccountInfo{account}
	return nil
}
