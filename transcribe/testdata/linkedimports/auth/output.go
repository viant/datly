package auth

import "github.com/viant/datly/transcribe/testdata/linkedimports/status"

// Output is an imported contract, not a transcription destination.
type Output struct {
	status.Status
	Subject string `json:"subject"`
}

func (*Output) ContractMethod() {}
