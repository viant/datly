package tests

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/viant/dsunit"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"os"
	"testing"
)

func InitDB(t *testing.T, configURI string, datasetURI, db string) bool {
	if !dsunit.InitFromURL(t, configURI) {
		return false
	}

	resourceExist, err := exists(datasetURI)
	if err != nil || !resourceExist {
		return true
	}

	initDataset := dsunit.NewDatasetResource(db, datasetURI, "", "")
	request := dsunit.NewPrepareRequest(initDataset)
	if !dsunit.Prepare(t, request) {
		return false
	}

	return true
}

func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
