package dml

import "fmt"

// ExecuteWithResult queues a statement in the existing transaction journal.
// The statement's identity is assigned during drain, independently of affected
// rows. It neither executes eagerly nor allocates or retries an identity.
func (d *Data) ExecuteWithResult(statement string, lastInsertID any, args ...any) error {
	if err := validateLastInsertID(lastInsertID); err != nil {
		return err
	}
	return d.append(dataOperation{kind: dataOpExecute, dml: statement,
		args: append([]any(nil), args...), lastInsertID: lastInsertID})
}

func validateLastInsertID(dest any) error {
	switch p := dest.(type) {
	case *int:
		if p != nil {
			return nil
		}
	case *int8:
		if p != nil {
			return nil
		}
	case *int16:
		if p != nil {
			return nil
		}
	case *int32:
		if p != nil {
			return nil
		}
	case *int64:
		if p != nil {
			return nil
		}
	}
	return fmt.Errorf("LastInsertId destination must be a nonnil signed integer pointer")
}

func assignLastInsertID(dest any, id int64) error {
	switch p := dest.(type) {
	case *int:
		if int64(int(id)) == id {
			*p = int(id)
			return nil
		}
	case *int8:
		if int64(int8(id)) == id {
			*p = int8(id)
			return nil
		}
	case *int16:
		if int64(int16(id)) == id {
			*p = int16(id)
			return nil
		}
	case *int32:
		if int64(int32(id)) == id {
			*p = int32(id)
			return nil
		}
	case *int64:
		*p = id
		return nil
	}
	return fmt.Errorf("LastInsertId %d overflows destination %T", id, dest)
}
