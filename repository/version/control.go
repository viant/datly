package version

const (
	ChangeKindModified = "modified"
	ChangeKindDeleted  = "deleted"
)

type (
	Version struct {
		SequenceChangeNumber int32 //sequence change number
	}

	//ChangeKind defines change types
	ChangeKind string
	Control    struct {
		Version
		ChangeKind ChangeKind
	}
)
