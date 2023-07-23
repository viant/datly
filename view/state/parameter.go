package state

import (
	"github.com/viant/datly/config"
	"github.com/viant/datly/shared"
)

type (
	Parameter struct {
		shared.Reference
		Fields       Parameters
		Predicate    *config.PredicateConfig
		Name         string `json:",omitempty"`
		PresenceName string `json:",omitempty"`

		In                *Location   `json:",omitempty"`
		Required          *bool       `json:",omitempty"`
		Description       string      `json:",omitempty"`
		DataType          string      `json:",omitempty"`
		Style             string      `json:",omitempty"`
		MaxAllowedRecords *int        `json:",omitempty"`
		MinAllowedRecords *int        `json:",omitempty"`
		ExpectedReturned  *int        `json:",omitempty"`
		Schema            *Schema     `json:",omitempty"`
		Output            *Codec      `json:",omitempty"`
		Const             interface{} `json:",omitempty"`
		DateFormat        string      `json:",omitempty"`
		ErrorStatusCode   int         `json:",omitempty"`
	}

	Parameters []*Parameters
	Location   struct {
		Kind Kind   `json:",omitempty"`
		Name string `json:",omitempty"`
	}

	Codec struct {
		shared.Reference
		Name      string `json:",omitempty"`
		Arguments []*NamedArgument

		config.CodecConfig
		Schema *Schema `json:",omitempty"`
	}

	NamedArgument struct {
		Name     string
		Position int
	}
)
