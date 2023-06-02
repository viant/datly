package async

import (
	"context"
	"database/sql"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"strings"
	"sync"
)

type (
	Jobs struct {
		mux   sync.Mutex
		index map[*sql.DB][]*JobQualifier
	}

	JobQualifier struct {
		PrincipalSubject *string
		ViewName         string
		JobID            *string
	}
)

func NewJobs() *Jobs {
	return &Jobs{index: map[*sql.DB][]*JobQualifier{}}
}

func (j *Jobs) AddJobs(db *sql.DB, qualifiers ...*JobQualifier) {
	if len(qualifiers) == 0 {
		return
	}

	j.mux.Lock()
	defer j.mux.Unlock()

	j.index[db] = append(j.index[db], qualifiers...)
}

func (j *Jobs) Index() map[*sql.DB][]*JobQualifier {
	return j.index
}
func BuildSelectSQL(ctx context.Context, db *sql.DB, qualifiers ...*JobQualifier) (string, []interface{}, error) {
	sb := &strings.Builder{}
	sb.WriteString("SELECT * FROM ")
	sb.WriteString(view.AsyncJobsTable)

	var args []interface{}

	for i, jobQualifier := range qualifiers {
		if i == 0 {
			sb.WriteString(" WHERE ")
		} else {
			sb.WriteString(" OR ")
		}

		sb.WriteString("(")
		sb.WriteString("MainView = ? ")
		args = append(args, jobQualifier.ViewName)
		sb.WriteString(" AND PrincipalSubject")
		if jobQualifier.PrincipalSubject != nil {
			sb.WriteString(" = ?")
			args = append(args, *jobQualifier.PrincipalSubject)
		} else {
			sb.WriteString(" IS NULL")
		}

		if jobQualifier.JobID != nil {
			sb.WriteString(" AND JobID = ?")
			args = append(args, *jobQualifier.JobID)
		}

		sb.WriteString(")")
	}

	return sb.String(), args, nil
}

func detectEscapeQuoteRune(ctx context.Context, db *sql.DB) (byte, error) {
	dialect, err := config.Dialect(ctx, db)
	if err != nil {
		return 0, err
	}

	return dialect.QuoteCharacter, nil
}
