package reader

import "strings"

type reservedMeta struct {
	or    bool
	and   bool
	where bool
	raw   bool
}

func hasKeyword(SQL, keyword string) *reservedMeta {
	return &reservedMeta{
		or:    strings.Contains(SQL, "$OR_"+keyword[1:]),
		and:   strings.Contains(SQL, "$AND_"+keyword[1:]),
		where: strings.Contains(SQL, "$WHERE_"+keyword[1:]),
		raw:   strings.Contains(SQL, keyword),
	}
}

func (r *reservedMeta) has() bool {
	return r.or || r.raw || r.and || r.where
}
