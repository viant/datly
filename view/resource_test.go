package view

import (
	"context"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/codec"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"testing"
	"time"
)

func TestNewResourceFromURL(t *testing.T) {
	Now = func() time.Time {
		aTime, _ := time.Parse(time.RFC3339Nano, "2022-07-13T19:18:02.063177478+02:00")
		return aTime
	}
	_ = toolbox.CreateDirIfNotExist("/tmp/view")
	//testLocation := toolbox.CallerDirectory(3)
	testLocation := ""

	testCases := []struct {
		description string
		url         string
		expect      string
	}{
		{
			url:    "case001",
			expect: `{"SourceURL":"testdata/case001/resource.yaml","Connectors":[{"Name":"mydb","Driver":"sqlite3"}],"Views":[{"Connector":{"Name":"mydb","Driver":"sqlite3"},"Name":"events","Alias":"t","Table":"events","Columns":[{"Name":"id","DataType":"Int"},{"Name":"quantity","DataType":"Float"},{"Name":"event_type_id","DataType":"Int"}],"CaseFormat":"lu","Selector":{"Constraints":{"Criteria":false,"OrderBy":false,"Limit":false,"Offset":false,"Projection":false}},"Template":{"Source":"events","Schema":{"Cardinality":"One"},"PresenceSchema":{"Cardinality":"One"}},"Schema":{"Cardinality":"One"},"MatchStrategy":"read_matched","Batch":{"Parent":10000},"Logger":{"Name":""},"Caser":5}],"ModTime":"2022-07-13T19:18:05+02:00"}`,
		},
	}

	for _, testCase := range testCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "testdata", "config.yaml")) {
			return
		}

		resource, err := NewResourceFromURL(context.TODO(), path.Join(testLocation, "testdata", testCase.url, "resource.yaml"), Types{}, codec.Visitors{})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assertly.AssertValues(t, testCase.expect, resource, testCase.description) {
			toolbox.DumpIndent(resource, true)
		}
	}
}
