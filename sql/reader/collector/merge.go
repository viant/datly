package collector

import (
	"context"
	"reflect"
	"sync"

	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// MergeData reconciles all ReadAll child collectors into their parents.
// Call on the root collector after all fetches complete.
func (r *Collector) MergeData() error {
	for i := range r.relations {
		if err := r.relations[i].MergeData(); err != nil {
			return err
		}
	}
	if r.parent == nil || !r.ReadAll() {
		return nil
	}
	return r.mergeToParent(nil)
}

// AssembleTrees materializes self-referencing views after ordinary relation
// merging. Children are assembled first so parent rows retain hydrated child
// graphs before their own flat rows become a tree.
func (r *Collector) AssembleTrees() error {
	if r == nil {
		return nil
	}
	for _, child := range r.relations {
		if err := child.AssembleTrees(); err != nil {
			return err
		}
	}
	plan := r.view.Tree
	if plan == nil {
		return nil
	}
	result, evidence := plan.build(r.DestPtr(), r.rowEvidence)
	r.setDest(result)
	if r.provenance {
		r.rowEvidence = evidence
	}
	if r.parent == nil {
		return nil
	}
	if err := r.resetParentHolder(nil); err != nil {
		return err
	}
	return r.mergeToParent(nil)
}

// RebindToParent refreshes a matched relation attachment after the child
// collector has been further hydrated (for example by nested read_all
// descendants). Streaming visitor attachment happens while scanning the child
// rows, so any later descendant merges would otherwise stay behind on the
// child collector copy instead of the parent-held relation values.
func (r *Collector) RebindToParent() error {
	if r == nil || r.parent == nil || r.ReadAll() {
		return nil
	}
	if err := r.resetParentHolder(nil); err != nil {
		return err
	}
	return r.mergeToParent(nil)
}

// RunOnRelation walks the collector tree and invokes the OnRelation hook on
// every materialized value that implements onRelationer.
func (r *Collector) RunOnRelation(ctx context.Context) error {
	var snapshot *hookTopology
	if r.provenance && r.hasRelationHooks() {
		snapshot = r.captureHookTopology()
	}
	if err := r.runOnRelation(ctx, snapshot); err != nil {
		return err
	}
	r.reconcileHookTopology(snapshot)
	return nil
}

func (r *Collector) runOnRelation(ctx context.Context, snapshot *hookTopology) error {
	for i := range r.relations {
		if err := r.relations[i].runOnRelation(ctx, snapshot); err != nil {
			return err
		}
	}
	dest := r.destValue
	ptr := xunsafe.AsPointer(dest.Interface())
	if ptr == nil || r.slice == nil {
		return nil
	}
	count := r.slice.Len(ptr)
	var targets []any
	if snapshot != nil {
		// Hooks run on originally fetched pointer targets, not on newly added
		// rows or live ordinals that a hook can reorder/truncate/nil.
		targets = snapshot.targets[r]
		count = len(targets)
	}
	visit := func(index int) error {
		var row any
		if targets != nil {
			row = targets[index]
		} else {
			row = r.slice.ValuePointerAt(ptr, index)
		}
		if row == nil || (reflect.ValueOf(row).Kind() == reflect.Pointer && reflect.ValueOf(row).IsNil()) {
			return nil
		}
		if r.view.Tree != nil {
			return r.view.Tree.VisitRow(row, func(row any) {
				if runRelationHook(ctx, row) && snapshot != nil {
					snapshot.called.Store(true)
				}
			})
		}
		if runRelationHook(ctx, row) && snapshot != nil {
			snapshot.called.Store(true)
		}
		return nil
	}
	concurrency := r.view.Spec.RelationalConcurrency
	if concurrency <= 1 || count <= 1 {
		for i := 0; i < count; i++ {
			if err := visit(i); err != nil {
				return err
			}
		}
		return r.rebindTreeAfterHooks(snapshot)
	}
	if concurrency > count {
		concurrency = count
	}
	jobs := make(chan int, count)
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	errors := make(chan error, concurrency)
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if err := visit(index); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		return err
	}
	return r.rebindTreeAfterHooks(snapshot)
}

func (r *Collector) rebindTreeAfterHooks(snapshot *hookTopology) error {
	if r.view.Tree == nil || r.parent == nil {
		return nil
	}
	if err := r.resetParentHolder(snapshot); err != nil {
		return err
	}
	return r.mergeToParent(snapshot)
}

func runRelationHook(ctx context.Context, value interface{}) bool {
	if hook, ok := value.(xhandler.OnRelationer); ok {
		hook.OnRelation(ctx)
		return true
	}
	return false
}

func (r *Collector) resetParentHolder(snapshot *hookTopology) error {
	if r == nil || r.parent == nil || r.relation == nil || r.relation.HolderField == nil || r.parent.slice == nil {
		return nil
	}
	holderField := r.relation.HolderField
	r.resetEvidenceHolder()
	zero := reflect.Zero(holderField.Type).Interface()
	parents := r.attachmentTargets(snapshot)
	for i := 0; i < parents.len(); i++ {
		parentValue, err := parents.at(i)
		if err != nil {
			return err
		}
		if parentValue == nil {
			continue
		}
		holderField.SetValue(xunsafe.AsPointer(parentValue), zero)
	}
	return nil
}

// mergeToParent stitches this relation's rows back onto the matching parent
// rows, supporting both composite and single-key joins.
func (r *Collector) mergeToParent(snapshot *hookTopology) error {
	links := r.relation.Of.On
	parents := r.attachmentTargets(snapshot)
	if r.relation.IsComposite() {
		destPtr := xunsafe.AsPointer(r.DestPtr())
		holderField := r.relation.HolderField
		valuePositions := r.parentCompositePositions(r.relation)

		for i := 0; i < r.slice.Len(destPtr); i++ {
			value := r.slice.ValuePointerAt(destPtr, i)
			keyParts := make([]interface{}, 0, len(links))
			for _, link := range links {
				key, err := r.linkKeyAt(value, link, i)
				if err != nil {
					return err
				}
				keyParts = append(keyParts, key)
			}
			positions, ok := valuePositions[buildCompositeKey(keyParts)]
			if !ok {
				continue
			}
			for _, position := range positions {
				parentValue, err := parents.at(position)
				if err != nil {
					return err
				}
				if parentValue == nil {
					continue
				}
				switch r.relation.Cardinality {
				case spec.CardinalityOne:
					at := r.slice.ValuePointerAt(destPtr, i)
					holderField.SetValue(xunsafe.AsPointer(parentValue), at)
					r.attachEvidence(position, i)
				case spec.CardinalityMany:
					r.Lock().Lock()
					holderSlice := r.relation.HolderSlice
					if holderSlice == nil {
						holderSlice = xunsafe.NewSlice(holderField.Type)
					}
					appender := holderSlice.Appender(holderField.ValuePointer(xunsafe.AsPointer(parentValue)))
					appendRelationHolderValue(appender, holderField.Type, value)
					r.Lock().Unlock()
					r.attachEvidence(position, i)
				}
			}
		}
		return nil
	}

	for i, link := range links {
		valuePositions := r.parentValuesPositions(r.relation.On[i].Namespace, r.relation.On[i].Column)
		destPtr := xunsafe.AsPointer(r.DestPtr())
		holderField := r.relation.HolderField
		for j := 0; j < r.slice.Len(destPtr); j++ {
			value := r.slice.ValuePointerAt(destPtr, j)
			key, err := r.linkKeyAt(value, link, j)
			if err != nil {
				return err
			}
			positions, ok := valuePositions[key]
			if !ok {
				continue
			}
			for _, position := range positions {
				parentValue, err := parents.at(position)
				if err != nil {
					return err
				}
				if parentValue == nil {
					continue
				}
				switch r.relation.Cardinality {
				case spec.CardinalityOne:
					at := r.slice.ValuePointerAt(destPtr, j)
					holderField.SetValue(xunsafe.AsPointer(parentValue), at)
					r.attachEvidence(position, j)
				case spec.CardinalityMany:
					r.Lock().Lock()
					holderSlice := r.relation.HolderSlice
					if holderSlice == nil {
						holderSlice = xunsafe.NewSlice(holderField.Type)
					}
					appender := holderSlice.Appender(holderField.ValuePointer(xunsafe.AsPointer(parentValue)))
					appendRelationHolderValue(appender, holderField.Type, value)
					r.Lock().Unlock()
					r.attachEvidence(position, j)
				}
			}
		}
	}
	return nil
}
