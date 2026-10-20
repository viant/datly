package engine

import "fmt"

func (c dmlCapability) InsertWithBatchSize(table string, rows any, size int, affected *int64) error {
	if err := c.guard.check("InsertWithBatchSize"); err != nil {
		return err
	}
	service, ok := c.service.(interface {
		InsertWithBatchSize(string, any, int, *int64) error
	})
	if !ok {
		return fmt.Errorf("native buffered insert batch capability unavailable")
	}
	return service.InsertWithBatchSize(table, rows, size, affected)
}

func (c dmlCapability) DeleteWithBatchSize(table string, rows any, size int, affected *int64) error {
	if err := c.guard.check("DeleteWithBatchSize"); err != nil {
		return err
	}
	service, ok := c.service.(interface {
		DeleteWithBatchSize(string, any, int, *int64) error
	})
	if !ok {
		return fmt.Errorf("native buffered delete batch capability unavailable")
	}
	return service.DeleteWithBatchSize(table, rows, size, affected)
}

// InsertWithBatchSizeAndOnDuplicateKeySQL forwards the authored conflict suffix through the invocation guard.
func (c dmlCapability) InsertWithBatchSizeAndOnDuplicateKeySQL(table string, rows any, size int, affected *int64, suffix string) error {
	if err := c.guard.check("InsertWithBatchSizeAndOnDuplicateKeySQL"); err != nil {
		return err
	}
	service, ok := c.service.(interface {
		InsertWithBatchSizeAndOnDuplicateKeySQL(string, any, int, *int64, string) error
	})
	if !ok {
		return fmt.Errorf("native buffered insert conflict batch capability unavailable")
	}
	return service.InsertWithBatchSizeAndOnDuplicateKeySQL(table, rows, size, affected, suffix)
}
