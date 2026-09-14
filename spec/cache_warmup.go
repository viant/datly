package spec

func (s *CacheWarmupSettings) Clone() *CacheWarmupSettings {
	if s == nil {
		return nil
	}
	cloned := *s
	cloned.FieldNames = append([]string(nil), s.FieldNames...)
	if s.Limit != nil {
		value := *s.Limit
		cloned.Limit = &value
	}
	if s.MaxCases != nil {
		value := *s.MaxCases
		cloned.MaxCases = &value
	}
	if s.Cases != nil {
		cloned.Cases = make([]*CacheWarmupCase, len(s.Cases))
		for i, item := range s.Cases {
			if item != nil {
				cloned.Cases[i] = item.Clone()
			}
		}
	}
	return &cloned
}

func (c *CacheWarmupCase) Clone() *CacheWarmupCase {
	if c == nil {
		return nil
	}
	cloned := *c
	cloned.FieldNames = append([]string(nil), c.FieldNames...)
	if c.Set != nil {
		cloned.Set = make([]*CacheWarmupParam, len(c.Set))
		for i, item := range c.Set {
			if item != nil {
				cloned.Set[i] = item.Clone()
			}
		}
	}
	return &cloned
}

func (p *CacheWarmupParam) Clone() *CacheWarmupParam {
	if p == nil {
		return nil
	}
	cloned := *p
	cloned.Values = append([]string(nil), p.Values...)
	return &cloned
}
