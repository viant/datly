package xgen

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildStructType_AddsVeltyNamesFromSQLColumns(t *testing.T) {
	rType := buildStructType([]columnDescriptor{
		{name: "IS_AUTH", dataType: "int"},
	}, true)
	require.NotNil(t, rType)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	field, ok := rType.FieldByName("IsAuth")
	require.True(t, ok)
	require.Equal(t, `names=IS_AUTH|IsAuth`, field.Tag.Get("velty"))
	require.Equal(t, `IS_AUTH`, field.Tag.Get("sqlx"))
}

func TestBuildStructType_DedupesVeltyNamesWhenGoFieldMatchesColumn(t *testing.T) {
	rType := buildStructType([]columnDescriptor{
		{name: "UserID", dataType: "int"},
	}, true)
	require.NotNil(t, rType)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	field, ok := rType.FieldByName("UserID")
	require.True(t, ok)
	require.Equal(t, `names=UserID`, field.Tag.Get("velty"))
}

func TestBuildStructType_OmitsVeltyWhenDisabled(t *testing.T) {
	rType := buildStructType([]columnDescriptor{
		{name: "USER_ID", dataType: "int"},
	}, false)
	require.NotNil(t, rType)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	field := rType.Field(0)
	require.Equal(t, "", field.Tag.Get("velty"))
	require.Equal(t, `USER_ID`, field.Tag.Get("sqlx"))
}
