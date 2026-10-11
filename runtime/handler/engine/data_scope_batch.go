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
