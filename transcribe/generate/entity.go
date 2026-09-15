package generate

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	xshape "github.com/viant/x/shape"
)

type EntityMethod struct {
	Receiver, Name, ValueType string
	Getter                    bool
	Signature                 string
}

// EntitySupportAsset is Go source shared by generated Go and Velty adapters.
// It contains typed accessors and an invocation-local original-input capturer.
type EntitySupportAsset struct {
	File                                      *ast.File
	CaptureFunction                           string
	Methods                                   []EntityMethod
	SnapshotType, SyncContextType, SyncMethod string
	Associations                              []EntityAssociation
	Invariants                                []EntityInvariant
}
type EntityInvariant struct {
	Identity                string
	Path                    []string
	Group, BackfillFunction string
}
type EntityAssociation struct {
	Identity                           string
	Path                               []string
	Field                              string
	StateType, KeyType, KeyAdapterType string
	CurrentKeyFunction                 string
}
type EntitySupportPlan struct {
	File                                      *ast.File
	Destination, CaptureFunction              string
	Methods                                   []EntityMethod
	SnapshotType, SyncContextType, SyncMethod string
	Associations                              []EntityAssociation
	Invariants                                []EntityInvariant
}

func (a *EntitySupportAsset) Clone() (*EntitySupportAsset, error) {
	if a == nil {
		return nil, nil
	}
	file, err := cloneGoFile(a.File)
	if err != nil {
		return nil, err
	}
	result := &EntitySupportAsset{File: file, CaptureFunction: a.CaptureFunction, Methods: append([]EntityMethod(nil), a.Methods...), SnapshotType: a.SnapshotType, SyncContextType: a.SyncContextType, SyncMethod: a.SyncMethod, Associations: append([]EntityAssociation(nil), a.Associations...)}
	for index := range result.Associations {
		result.Associations[index].Path = append([]string(nil), a.Associations[index].Path...)
	}
	result.Invariants = append([]EntityInvariant(nil), a.Invariants...)
	for index := range result.Invariants {
		result.Invariants[index].Path = append([]string(nil), a.Invariants[index].Path...)
	}
	return result, nil
}

func (r *planResolver) resolveEntitySupport() error {
	asset := r.input.EntitySupport
	if asset == nil {
		return nil
	}
	if asset.File == nil || !token.IsIdentifier(asset.CaptureFunction) {
		return fmt.Errorf("generated entity support requires a source file and capture function")
	}
	r.plan.EntitySupport = &EntitySupportPlan{File: asset.File, Destination: r.plan.Generation.File("entities", "entities.go"), CaptureFunction: asset.CaptureFunction, Methods: append([]EntityMethod(nil), asset.Methods...), SnapshotType: asset.SnapshotType, SyncContextType: asset.SyncContextType, SyncMethod: asset.SyncMethod, Associations: append([]EntityAssociation(nil), asset.Associations...)}
	for index := range r.plan.EntitySupport.Associations {
		r.plan.EntitySupport.Associations[index].Path = append([]string(nil), asset.Associations[index].Path...)
	}
	r.plan.EntitySupport.Invariants = append([]EntityInvariant(nil), asset.Invariants...)
	for index := range r.plan.EntitySupport.Invariants {
		r.plan.EntitySupport.Invariants[index].Path = append([]string(nil), asset.Invariants[index].Path...)
	}
	return nil
}

func (p *EntitySupportPlan) source(packageName string) (string, error) {
	file, err := cloneGoFile(p.File)
	if err != nil {
		return "", err
	}
	file.Name.Name = packageName
	source, err := (xshape.SourceParser{}).FormatFile(file)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(source)) + "\n", nil
}
