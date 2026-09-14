package collector

import "github.com/viant/xunsafe"

// WaitIfNeeded blocks until the parent collector signals that its rows are
// scanned (used by child collectors that need parent positions first).
func (r *Collector) WaitIfNeeded() {
	if r.parent != nil {
		r.parent.wg.Wait()
	}
}

// Fetched signals that row scanning for this collector is complete.
func (r *Collector) Fetched() {
	r.prepareRelationIndexes()
	if r.wgDelta > 0 {
		r.wg.Done()
		r.wgDelta--
	}
}

// Unlock signals to dependents that this relation's data is ready.
func (r *Collector) Unlock() {
	if r.parent == nil {
		return
	}
	r.parent.dataSync.Delete(r.relation.Holder)
}

// OnSkip drops the last accumulated row when a record is skipped.
func (r *Collector) OnSkip(_ []interface{}) error {
	sliceLen := r.slice.Len(xunsafe.AsPointer(r.DestPtr()))
	if sliceLen > 0 {
		if err := r.appender.Trunc(sliceLen - 1); err != nil {
			return err
		}
	}
	r.truncateUnmapped()
	return nil
}

// OnSkipUnmapped rolls back only resolver-owned hidden columns. Codec scan
// rows are not appended to the typed destination until after SQLx emits them.
func (r *Collector) OnSkipUnmapped(_ []interface{}) error {
	r.truncateUnmapped()
	return nil
}

func (r *Collector) truncateUnmapped() {
	for _, values := range r.values {
		if values != nil && len(*values) > 0 {
			*values = (*values)[:len(*values)-1]
		}
	}
}
