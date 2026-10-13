package pipeline

import dqlstmt "github.com/viant/datly/repository/shape/dql/statement"

type Decision struct {
	HasRead    bool
	HasExec    bool
	HasUnknown bool
}

func Classify(statements dqlstmt.Statements) Decision {
	var ret Decision
	for _, stmt := range statements {
		if stmt == nil {
			continue
		}
		if stmt.Kind == dqlstmt.KindExec || stmt.Kind == dqlstmt.KindService {
			ret.HasExec = true
			continue
		}
		if stmt.Kind == dqlstmt.KindRead {
			ret.HasRead = true
			continue
		}
		ret.HasUnknown = true
	}
	return ret
}
