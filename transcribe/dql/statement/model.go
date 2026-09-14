package statement

type Kind string

const (
	KindUnknown Kind = "unknown"
	KindRead    Kind = "read"
	KindExec    Kind = "exec"
	KindService Kind = "service"
)

type Statement struct {
	Start            int
	End              int
	SQLStart         int
	SQLEnd           int
	TemplateBalanced bool
	OperationStart   int
	Kind             Kind
	Operation        string
}

func (s *Statement) IsExecutable() bool {
	return s != nil && (s.Kind == KindExec || s.Kind == KindService)
}

type Statements []*Statement

type Classification struct {
	HasRead    bool
	HasExec    bool
	HasService bool
	HasUnknown bool
}

func (s Statements) Classify() Classification {
	var result Classification
	for _, item := range s {
		if item == nil {
			continue
		}
		switch item.Kind {
		case KindRead:
			result.HasRead = true
		case KindExec:
			result.HasExec = true
		case KindService:
			result.HasService = true
		default:
			result.HasUnknown = true
		}
	}
	return result
}
