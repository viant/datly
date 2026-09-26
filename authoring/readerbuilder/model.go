// Package readerbuilder provides stateless, source-preserving DQL reader edits.
package readerbuilder

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/dql"
)

type OperationType string

const (
	OperationInspect                    OperationType = "inspect"
	OperationCreateReader               OperationType = "createReader"
	OperationSetPackage                 OperationType = "setPackage"
	OperationAddField                   OperationType = "addField"
	OperationUpdateField                OperationType = "updateField"
	OperationRemoveField                OperationType = "removeField"
	OperationAddFieldPredicate          OperationType = "addFieldPredicate"
	OperationUpdateFieldPredicate       OperationType = "updateFieldPredicate"
	OperationRemoveFieldPredicate       OperationType = "removeFieldPredicate"
	OperationUpdatePredicateGroup       OperationType = "updatePredicateGroup"
	OperationUpdatePredicateComposition OperationType = "updatePredicateComposition"
	OperationAddFunction                OperationType = "addFunction"
	OperationUpdateFunction             OperationType = "updateFunction"
	OperationRemoveFunction             OperationType = "removeFunction"
	OperationSetSetting                 OperationType = "setSetting"
	OperationAddView                    OperationType = "addView"
	OperationUpdateView                 OperationType = "updateView"
	OperationRemoveView                 OperationType = "removeView"
	OperationUpdateRelation             OperationType = "updateRelation"
	OperationSetColumnRole              OperationType = "setColumnRole"
	OperationSetColumnContract          OperationType = "setColumnContract"
	OperationBatch                      OperationType = "batch"
)

type Request struct {
	DQL       string    `json:"dql"`
	Operation Operation `json:"operation"`
}

type Operation struct {
	Type                 OperationType                 `json:"type"`
	Reader               *ReaderMutation               `json:"reader,omitempty"`
	Package              *PackageMutation              `json:"package,omitempty"`
	Field                *Field                        `json:"field,omitempty"`
	Predicate            *PredicateMutation            `json:"predicate,omitempty"`
	PredicateGroup       *PredicateGroupMutation       `json:"predicateGroup,omitempty"`
	PredicateComposition *PredicateCompositionMutation `json:"predicateComposition,omitempty"`
	Function             *FunctionMutation             `json:"function,omitempty"`
	Setting              *SettingMutation              `json:"setting,omitempty"`
	View                 *ViewMutation                 `json:"view,omitempty"`
	Relation             *RelationMutation             `json:"relation,omitempty"`
	ColumnRole           *ColumnRoleMutation           `json:"columnRole,omitempty"`
	Column               *ColumnContractMutation       `json:"column,omitempty"`
	Operations           []Operation                   `json:"operations,omitempty"`
}

// PredicateGroupMutation updates the compiled boolean operator for every
// explicit expansion site of one predicate group. Views must name the complete
// current expansion scope so an authoring client cannot silently broaden or
// narrow a shared filter group.
type PredicateGroupMutation struct {
	Group    int      `json:"group"`
	Views    []string `json:"views"`
	Operator string   `json:"operator"`
}

// PredicateCompositionMutation changes only outer boolean composition. Every
// existing expansion must occur exactly as often as before; group membership
// and within-group operators are managed by the predicate/group operations.
type PredicateCompositionMutation struct {
	View         string                     `json:"view"`
	Occurrence   int                        `json:"occurrence"`
	Terms        []PredicateCompositionTerm `json:"terms"`
	BuildKeyword string                     `json:"buildKeyword"`
}

type PredicateCompositionTerm struct {
	Operator  string `json:"operator"`
	Connector string `json:"connector"`
	Groups    []int  `json:"groups"`
}

// PredicateComposition describes one complete Builder chain, in source order.
// Connector is the effective persistent And/Or state at each Combine call.
// The first term also records that state, although no preceding term exists.
type PredicateComposition struct {
	View         string                     `json:"view"`
	Occurrence   int                        `json:"occurrence"`
	Terms        []PredicateCompositionTerm `json:"terms,omitempty"`
	BuildKeyword string                     `json:"buildKeyword"`
	SourceSpan   dql.SourceSpan             `json:"sourceSpan"`
	Editable     bool                       `json:"editable"`
	Reason       string                     `json:"reason,omitempty"`
}

type PackageMutation struct {
	Path     string `json:"path"`
	Expected string `json:"expected,omitempty"`
}

// ReaderMutation defines the initial typed root graph. The reader builder owns
// DQL rendering and compilation; clients provide author intent, not DQL text.
type ReaderMutation struct {
	Package    string `json:"package"`
	Connector  string `json:"connector"`
	Route      string `json:"route"`
	Name       string `json:"name"`
	TypeName   string `json:"typeName,omitempty"`
	OutputName string `json:"outputName,omitempty"`
	SQL        string `json:"sql"`
}

type Field struct {
	ExistingName        string         `json:"existingName,omitempty"`
	Name                string         `json:"name"`
	Type                string         `json:"type"`
	SourceKind          string         `json:"sourceKind"`
	SourceName          string         `json:"sourceName"`
	Required            *bool          `json:"required,omitempty"`
	QuerySelector       string         `json:"querySelector,omitempty"`
	UpdateQuerySelector *string        `json:"updateQuerySelector,omitempty"`
	Value               *string        `json:"value,omitempty"`
	UpdateValue         bool           `json:"updateValue,omitempty"`
	URI                 *string        `json:"uri,omitempty"`
	UpdateURI           bool           `json:"updateUri,omitempty"`
	Codec               *CodecMutation `json:"codec,omitempty"`
	UpdateCodec         bool           `json:"updateCodec,omitempty"`
	EmitOutput          *bool          `json:"emitOutput,omitempty"`
	Description         *string        `json:"description,omitempty"`
	UpdateDescription   bool           `json:"updateDescription,omitempty"`
	Example             *string        `json:"example,omitempty"`
	UpdateExample       bool           `json:"updateExample,omitempty"`
}

type CodecMutation struct {
	Name string   `json:"name"`
	Args []string `json:"args,omitempty"`
}

type PredicateMutation struct {
	Field           string   `json:"field"`
	Occurrence      int      `json:"occurrence,omitempty"`
	View            string   `json:"view"`
	Group           int      `json:"group"`
	Name            string   `json:"name,omitempty"`
	Args            []string `json:"args,omitempty"`
	ApplyWhenAbsent bool     `json:"applyWhenAbsent,omitempty"`
	ExpansionViews  []string `json:"expansionViews,omitempty"`
	GroupOperator   string   `json:"groupOperator,omitempty"`
	CombineOperator string   `json:"combineOperator,omitempty"`
}

type FunctionMutation struct {
	Name         string   `json:"name"`
	Args         []string `json:"args,omitempty"`
	ExpectedArgs []string `json:"expectedArgs,omitempty"`
	Occurrence   int      `json:"occurrence,omitempty"`
}

type SettingMutation struct {
	Name    string             `json:"name"`
	Args    []string           `json:"args,omitempty"`
	Options []FunctionMutation `json:"options,omitempty"`
	Remove  bool               `json:"remove,omitempty"`
}

type ViewMutation struct {
	Name     string            `json:"name"`
	Kind     spec.RelationKind `json:"kind,omitempty"`
	TypeExpr string            `json:"typeExpr,omitempty"`
	SQL      string            `json:"sql,omitempty"`
	Parent   string            `json:"parent,omitempty"`
	Join     string            `json:"join,omitempty"`
	On       string            `json:"on,omitempty"`
}

type RelationMutation struct {
	Name   string `json:"name"`
	Parent string `json:"parent"`
	On     string `json:"on"`
}

type ColumnRoleMutation struct {
	View   string `json:"view"`
	Column string `json:"column"`
	Role   string `json:"role"`
}

// ColumnContractMutation applies cohesive field metadata to one compiled view
// column. A nil CastType preserves the current CAST; a non-nil empty value
// removes it. Tags are merged by key and RemoveTags removes only the named
// metadata, preserving unrelated authored tags.
type ColumnContractMutation struct {
	View       string            `json:"view"`
	Column     string            `json:"column"`
	CastType   *string           `json:"castType,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	RemoveTags []string          `json:"removeTags,omitempty"`
}

type Response struct {
	Applied     bool                     `json:"applied"`
	DQL         string                   `json:"dql"`
	Structure   *Structure               `json:"structure,omitempty"`
	Diagnostics []*transcribe.Diagnostic `json:"diagnostics,omitempty"`
}

type Structure struct {
	Status                string                      `json:"status"`
	Component             *spec.Component             `json:"component,omitempty"`
	Declarations          []dql.DeclarationOccurrence `json:"declarations,omitempty"`
	Views                 []ViewOccurrence            `json:"views,omitempty"`
	PredicateExpansions   []PredicateExpansion        `json:"predicateExpansions,omitempty"`
	PredicateCompositions []PredicateComposition      `json:"predicateCompositions,omitempty"`
	Functions             []FunctionOccurrence        `json:"functions,omitempty"`
	ColumnContracts       []ColumnContract            `json:"columnContracts,omitempty"`
	AvailableConnectors   []string                    `json:"availableConnectors,omitempty"`
	AvailablePredicates   []string                    `json:"availablePredicates,omitempty"`
	AvailableCaches       []string                    `json:"availableCaches,omitempty"`
}

type FunctionOccurrence struct {
	Name       string         `json:"name"`
	Args       []string       `json:"args,omitempty"`
	Occurrence int            `json:"occurrence"`
	SourceSpan dql.SourceSpan `json:"sourceSpan"`
}

// ColumnContract is the normalized authoring projection for one view column.
// It lets clients render cohesive controls without parsing DQL or Go tags.
type ColumnContract struct {
	View     string            `json:"view"`
	Column   string            `json:"column"`
	CastType string            `json:"castType,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

type ViewOccurrence struct {
	Name string `json:"name"`
	// SQL is the exact embedded query for this named view. It intentionally
	// excludes the outer graph projection and join wrapper so authoring clients
	// can edit a view without reverse-engineering compiled runtime SQL.
	SQL string `json:"sql,omitempty"`
	// SourceProjectionAll is true only for a single-table SELECT * without
	// exclusions, joins, or computed projection fields. Authoring clients may
	// then use connector columns as a complete fallback for an unresolved view.
	SourceProjectionAll bool           `json:"sourceProjectionAll,omitempty"`
	SourceSpan          dql.SourceSpan `json:"sourceSpan"`
}

type PredicateExpansion struct {
	View       string         `json:"view"`
	Method     string         `json:"method"`
	Group      int            `json:"group"`
	Operator   string         `json:"operator,omitempty"`
	SourceSpan dql.SourceSpan `json:"sourceSpan"`
}
